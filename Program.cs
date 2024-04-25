using Confluent.Kafka;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using DevelopmentActivity.Producer.Shared;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using static Confluent.Kafka.ConfigPropertyNames;

namespace DevelopmentActivity.Producer
{
    internal class Program
    {
        static Kafka.DataService _kafkaDataService = new Kafka.DataService();
        static ElasticSearch.DataService _elasticsearchDataService = new ElasticSearch.DataService();
        static WebAPI.DataService _webapiDataService = new WebAPI.DataService();
        private static ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);

        static async Task MainLoop()
        {
            try
            {
                var jsonDataStr = await _webapiDataService.FetchData(WebAPI.Config.DataUrl);
                Logger.Log($"PROG", $"Current API data is {jsonDataStr.Length} bytes [{jsonDataStr.md5()}]");

                if (ElasticSearch.Config.Enabled)
                {
                    Logger.Log("PROG", "Comparing to Last Elastic Datapoint");
                    var elasticIndexResult = await _elasticsearchDataService.VerifyContainerExists();
                    if (elasticIndexResult == RequestResult.OK)
                    {
                        var elasticCompareResult = await _elasticsearchDataService.CompareToDataStore(jsonDataStr);

                        if (elasticCompareResult == RequestResult.Change)
                        {
                            Logger.Log("PROG", "Data has changed, producing new data to Elastic");
                            await _elasticsearchDataService.ProduceToDataStore(jsonDataStr);
                        }
                        else if (elasticCompareResult == RequestResult.Error)
                        {
                            Logger.Log("PROG", "Error when comparing data, no action taken");
                        }
                    }
                }
                if (Kafka.Config.Enabled)
                {
                    Logger.Log("PROG", "Comparing to Last Kafka Datapoint");
                    var kafkaCompareResult = await _kafkaDataService.CompareToDataStore(jsonDataStr);

                    if (kafkaCompareResult == RequestResult.Change)
                    {
                        Logger.Log("PROG", "Data has changed, producing new data to Kafka");
                        await _kafkaDataService.ProduceToDataStore(jsonDataStr);
                    }
                    else if (kafkaCompareResult == RequestResult.Error)
                    {
                        Logger.Log("PROG", "Error when comparing data, no action taken");
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Log("PROG", $"Error {ex.Message}");
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║        [DevelopmentActivity.Producer] Module      ║");
            Console.WriteLine("║                                                   ║");
            Console.WriteLine("║                Strong Towns Langley               ║");
            Console.WriteLine("║            DevelopmentActivity Analytics          ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝");

            Logger.Log("PROG", "Program starting");
            ConfigManager.LoadConfiguration(typeof(ElasticSearch.Config), "ElasticSearch");
            ConfigManager.LoadConfiguration(typeof(Kafka.Config), "Kafka");
            ConfigManager.LoadConfiguration(typeof(WebAPI.Config), "WebAPI");

            var timer = new Timer(async _ =>
            {
                Logger.Log("PROG", "Updating data");
                await MainLoop();
                Logger.Log("PROG", $"Pausing for {WebAPI.Config.IntervalMinutes} minutes");
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(WebAPI.Config.IntervalMinutes));

            _waitHandle.Wait(); // Wait indefinitely, keeping the main thread alive
        }
    }
}