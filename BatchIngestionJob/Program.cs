using System;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Batch.Common;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BatchCreation
{
    public class Program
    {
        public string BatchAccountName = "testengie";
        string BatchAccountKey  = "";
        string BatchAccountUrl  = "";
        string BatchResourceUri = "";
        string AuthorityUri = "";
        string ClientId = "";
        string ClientKey = "";
        public static void Main(string[] args)
        {
           Program p = new Program();
           p.RunAsync().Wait();
        }
        public async Task RunAsync()
        {
           string PoolId = "engie";
           string JobId = "ingestion";

            Console.WriteLine("Sample start: {0}", DateTime.Now);

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            Func<Task<string>> tokenProvider = () => GetAuthenticationTokenAsync();

            using (BatchClient client = await BatchClient.OpenAsync(new BatchTokenCredentials(BatchAccountUrl, tokenProvider)))
            {
                Console.WriteLine("Creating pool [{0}]...", PoolId);

                ImageReference imageReference = new ImageReference("/subscriptions/e243327e-b18c-4766-8f44-d9a945082e57/resourceGroups/engie/providers/Microsoft.Compute/images/engieimg3");                    

                ContainerConfiguration containerConfig = new ContainerConfiguration(); 

                VirtualMachineConfiguration virtualMachineConfiguration =
                    new VirtualMachineConfiguration(imageReference: imageReference, nodeAgentSkuId: "batch.node.ubuntu 16.04");

                virtualMachineConfiguration.ContainerConfiguration = containerConfig;

                CloudPool pool = client.PoolOperations.CreatePool(
                    poolId: PoolId,
                    virtualMachineSize: "Standard_D11_v2",
                    virtualMachineConfiguration: virtualMachineConfiguration,
                    targetDedicatedComputeNodes: 1);

                    pool.MaxTasksPerComputeNode = 2;
                    pool.TaskSchedulingPolicy = new TaskSchedulingPolicy(ComputeNodeFillType.Spread);         
                
                // Commit pool creation
                //pool.Commit();

                // Simple task command
                CloudJob job = client.JobOperations.CreateJob();
                job.Id = JobId;
                job.PoolInformation = new PoolInformation { PoolId = PoolId };
                job.Commit();

                TaskContainerSettings taskContainerSettings = new TaskContainerSettings (
                     imageName: "scoriani/ingestion", containerRunOptions: "--env DOTNET_SKIP_FIRST_TIME_EXPERIENCE=true --rm");

                List<CloudTask> tasks = new List<CloudTask>();
                CloudTask containerTask;
                string cmdLine;

                // Storage container root
                string containerName="work3";

                // Starting from
                DateTime startDate = DateTime.Parse("2017-01-01");

                // For a given amount of days
                for (int i=1; i<2; i++)
                {
                    cmdLine = String.Format("{0} {1} {2}",startDate.ToString("yyyy-MM-dd"),startDate.ToString("yyyy-MM-dd"),containerName);
                    containerTask = new CloudTask ("Task1-"+i.ToString(),cmdLine);
                    containerTask.ContainerSettings = taskContainerSettings;
                    tasks.Add(containerTask);

                    startDate = startDate.AddDays(1);
                }

                client.JobOperations.AddTask(JobId, tasks);

                TimeSpan timeout = TimeSpan.FromMinutes(1800);

                Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                IEnumerable<CloudTask> addedTasks = client.JobOperations.ListTasks(JobId);

                client.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);

                Console.WriteLine("All tasks reached state Completed.");

                Console.WriteLine();

                Console.WriteLine("Printing task output...");

                IEnumerable<CloudTask> completedtasks = client.JobOperations.ListTasks(JobId);

                foreach (CloudTask task in completedtasks)
                 {
                     string nodeId = String.Format(task.ComputeNodeInformation.ComputeNodeId);

                     Console.WriteLine("Task: {0}", task.Id);

                     Console.WriteLine("Node: {0}", nodeId);

                     Console.WriteLine("Standard out:");
                     Console.WriteLine(task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
                 }

                 Console.WriteLine("Sample end: {0}", DateTime.Now);
                
                client.JobOperations.DeleteJob(JobId);
                //client.PoolOperations.DeletePool(PoolId);
            }            
        }
        public async Task<string> GetAuthenticationTokenAsync()
        {
            AuthenticationContext authContext = new AuthenticationContext(AuthorityUri);
            AuthenticationResult authResult = await authContext.AcquireTokenAsync(BatchResourceUri, new ClientCredential(ClientId, ClientKey));

            return authResult.AccessToken;
        }
    }
}
