using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Configuration;
using System.Collections;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Generation
{
    class Program
    {
        int startCustIds;
        int endCustIds;
        int startSiteIdsPerCust;
        int endSiteIdsPerCust;
        int numDays;
        DateTime startDate;

        String containerName;
        static void Main(string[] args)
        {
            var p = new Program();

            Console.WriteLine("Start time: {0}", DateTime.Now.ToString());

            p.startCustIds = Int32.Parse(args[0]);
            p.endCustIds = Int32.Parse(args[1]);
            p.startSiteIdsPerCust = Int32.Parse(args[2]);
            p.endSiteIdsPerCust = Int32.Parse(args[3]);
            p.numDays = Int32.Parse(args[4]);
            p.startDate = DateTime.Parse(args[5]);
            p.containerName = args[6];

            p.RunAsync().Wait();

            Console.WriteLine("End time: {0}", DateTime.Now.ToString());
        }

        private async Task RunAsync()
        {
            string containerSourceName = "source";
            string containerDestName = containerName;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer containerSrc = blobClient.GetContainerReference(containerSourceName);
            CloudBlobContainer containerDest = blobClient.GetContainerReference(containerDestName);
            BlobRequestOptions requestOptions = new BlobRequestOptions() { RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), 3) };

            try
            {
                await containerSrc.CreateIfNotExistsAsync(requestOptions, null);
                await containerDest.CreateIfNotExistsAsync(requestOptions, null);
            }
            catch (StorageException se)
            {
                Console.WriteLine(se.Message);
            }

            // Read from sample Daily readings file

            MemoryStream ms = new MemoryStream();

            CloudBlockBlob blobSrc = containerSrc.GetBlockBlobReference("108895_001.json");
            String input = await blobSrc.DownloadTextAsync();

            JObject o = JObject.Parse(input);

            // Insert creation tasks
            var tasks = new List<Task>();
            int totTasks = 0;

            Random r = new Random();

            // Number of days
            for (var d=0; d<numDays; d++)
            {   // Number of customers
                for (var i = startCustIds; i < endCustIds; i++)
                {
                    // Number of sites per customer
                    for (var z = startSiteIdsPerCust; z < endSiteIdsPerCust+1; z++)
                    {
                        o["custid"] = i.ToString("D5");
                        o["siteid"] = z.ToString("D5");
                        o["startdate"] = startDate.AddDays(d).ToString("s");
                        o["enddate"] = startDate.AddDays(d).AddHours(23).AddMinutes(50).ToString("s");

                        totTasks++;
                        tasks.Add(this.CreateDailyReadings(containerDest,totTasks, o));
                    }
                }
            }            
            await Task.WhenAll(tasks);
        }
 

        private async Task CreateDailyReadings(CloudBlobContainer dest, int taskId, JObject sampleJson)
        {
            String strJson = sampleJson.ToString(Formatting.None);
            DateTime startDate = DateTime.Parse(sampleJson["startdate"].ToString());    
            BlobRequestOptions requestOptions = new BlobRequestOptions() { RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(3), 5) };

            string fileName = startDate.ToString("yyyyMMdd")+"/"+sampleJson["custid"]+"_"+sampleJson["siteid"]+".json";

            try
            {
                CloudBlockBlob blobDst = dest.GetBlockBlobReference(fileName);
                await blobDst.UploadTextAsync(sampleJson.ToString(Formatting.None),null, requestOptions, null);
            }
            catch (StorageException se)
            {
                Console.WriteLine(se.Message);
            }
        }

    }
}
