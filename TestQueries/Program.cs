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
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace testqueries
{
    class Program
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

        static void Main(string[] args)
        {
            Stopwatch s = new Stopwatch();
            s.Start();
            RunAsync().Wait();
            s.Stop();
            Console.WriteLine("Total execution time: {0} mS",s.ElapsedMilliseconds.ToString());
        }

        static async Task RunAsync()
        {
            string endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            string authKey = ConfigurationManager.AppSettings["AuthorizationKey"];

            using (var docClient = new DocumentClient(new Uri(endpoint), authKey, ConnectionPolicy))
            {
                IDocumentQuery<dynamic> query = docClient.CreateDocumentQuery(UriFactory.CreateDocumentCollectionUri(DatabaseName, DataCollectionName), 
                "SELECT c['10minpoints'], c['startdate'] FROM c WHERE c.partitionKey = '00085_00001'", 
                new FeedOptions 
                { 
                    PopulateQueryMetrics = true, 
                    MaxItemCount = -1, 
                    MaxDegreeOfParallelism = -1, 
                    EnableCrossPartitionQuery = true 
                }).AsDocumentQuery();

                FeedResponse<dynamic> result = await query.ExecuteNextAsync();

                // Returns metrics by partition key range Id 
                IReadOnlyDictionary<string, QueryMetrics> metrics = result.QueryMetrics;

                Console.WriteLine("\n Query completed with {0} RUs", result.RequestCharge);

                PrintResults(result);

                PrintMetrics(metrics);

            }            
        }

        static void PrintResults(FeedResponse<dynamic> result)
        {
            Console.WriteLine("\n============== Results =============\n");

            foreach(var res in result)
            {
                Console.WriteLine(res.ToString());
            }
            Console.WriteLine("\n");
        }

        static void PrintMetrics(IReadOnlyDictionary<string, QueryMetrics> metrics)
        {
            Console.WriteLine("============== Metrics =============\n");

            foreach(var partition in metrics)
            {
                Console.WriteLine("Part: {0} TotTime: {1} RetrDoc: {2} OutputDoc: {3} IndexHitRatio: {4} Retries: {5} ",  
                                                                                            partition.Key, 
                                                                                            partition.Value.TotalTime, 
                                                                                            partition.Value.RetrievedDocumentCount, 
                                                                                            partition.Value.OutputDocumentCount,
                                                                                            partition.Value.IndexHitRatio,
                                                                                            partition.Value.Retries);                                                                                            
            }
            Console.WriteLine("\n");
        }
    }
}
