namespace Ingestion
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Configuration;
    using System.Diagnostics;
    using System.Net;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Core;
    using Microsoft.WindowsAzure.Storage.Shared;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public sealed class Program
    {
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string DataCollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy 
        { 
            ConnectionMode = ConnectionMode.Gateway, 
            ConnectionProtocol = Protocol.Https, 
            RequestTimeout = new TimeSpan(1, 0, 0), 
            MaxConnectionLimit = 1000, 
            RetryOptions = new RetryOptions 
            { 
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            } 
        };

        private static readonly string InstanceId = Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;
        private const int MinThreadPoolSize = 100;

        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();
        private DocumentClient docClient;

        private static DateTime startDate;
        private static DateTime endDate;
        private static String containerSourceName;

        private int currentCollectionThroughput = 0;

        private Program(DocumentClient client)
        {
            this.docClient = client;
        }

        public static void Main(string[] args)
        {
            startDate = DateTime.Parse(args[0]);
            endDate = DateTime.Parse(args[1]);
            containerSourceName = args[2];
                          
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            string endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            string authKey = ConfigurationManager.AppSettings["AuthorizationKey"];

            Console.WriteLine("Start time: {0}", DateTime.Now.ToString());

            try
            {
                using (var docClient = new DocumentClient(
                    new Uri(endpoint),
                    authKey,
                    ConnectionPolicy))
                {
                    var program = new Program(docClient);
                    program.RunAsync().Wait();
                    Console.WriteLine("End time: {0}", DateTime.Now.ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Ingestion failed with exception:{0}", e);
            }
        }

        private async Task RunAsync()
        {       
            // Blob storage client
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["StorageConnectionString"]);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerSourceName);
            BlobRequestOptions requestOptions = new BlobRequestOptions() { RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), 3) };

            // CosmosDB client
            DocumentCollection dataCollection = GetCollectionIfExists(DatabaseName, DataCollectionName);
            OfferV2 offer = (OfferV2)docClient.CreateOfferQuery().Where(o => o.ResourceLink == dataCollection.SelfLink).AsEnumerable().FirstOrDefault();
            currentCollectionThroughput = offer.Content.OfferThroughput;

            int degreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
            int taskCount;

            if (degreeOfParallelism == -1)
            {
                // set TaskCount = 10 for each 10k RUs, minimum 1, maximum 250
                taskCount = Math.Max(currentCollectionThroughput / 1000, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = degreeOfParallelism;
            }


            // List all blobs in a day
            BlobContinuationToken continuationToken = null;
            bool useFlatBlobListing = true;
            BlobListingDetails blobListingDetails = BlobListingDetails.None;
            // blob per request determined by parallelism
            int maxBlobsPerRequest = 73000/taskCount;
            List<IListBlobItem> listBlob = new List<IListBlobItem>();

            var dir = container.GetDirectoryReference(startDate.ToString("yyyyMMdd"));

            var tasks = new List<Task>();

            do
            {
                var listResult = await dir.ListBlobsSegmentedAsync(useFlatBlobListing, blobListingDetails, maxBlobsPerRequest, continuationToken, null, null);
                continuationToken = listResult.ContinuationToken;
                listBlob.AddRange(listResult.Results);

                tasks.Add(this.InsertDocument(blobClient, docClient, dataCollection, listResult.Results));
            } 
            while (continuationToken != null);

            await Task.WhenAll(tasks);
        }

        private async Task InsertDocument(CloudBlobClient blobClient, DocumentClient docClient, DocumentCollection collection, IEnumerable<IListBlobItem> blobs)
        {

            string dirName;
            string blobName;
            
            foreach(IListBlobItem b in blobs)
            {
                dirName = b.Uri.Segments[2].Substring(0,8);
                blobName = b.Uri.Segments[3].Substring(0,16);

                var container = blobClient.GetContainerReference(containerSourceName);
                var dir = container.GetDirectoryReference(dirName);

                CloudBlockBlob blobSrc = dir.GetBlockBlobReference(blobName);
                String doc = await blobSrc.DownloadTextAsync();

                Dictionary<string, object> docDict = JsonConvert.DeserializeObject<Dictionary<string, object>>(doc);

                docDict["partitionKey"] = docDict["custid"]+ "_" + docDict["siteid"];

                try
                {                
                    ResourceResponse<Document> response = await docClient.CreateDocumentAsync(
                    UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollectionName),
                    docDict,
                    new RequestOptions() { }); 
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(docDict), e);
                }
            }
        }
        private async Task<DocumentCollection> CreatePartitionedCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add(ConfigurationManager.AppSettings["CollectionPartitionKey"]);

            return await docClient.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName), 
                    collection, 
                    new RequestOptions { OfferThroughput = currentCollectionThroughput });
        }
        private Database GetDatabaseIfExists(string databaseName)
        {
            return docClient.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return docClient.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }
    }
}
