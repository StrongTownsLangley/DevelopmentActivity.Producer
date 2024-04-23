using Confluent.Kafka;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
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
    // Extentions
    public static class StringExtensions
    {
        public static string md5(this string input)
        {
            using (MD5 md5 = MD5.Create())
            {
                byte[] inputBytes = Encoding.ASCII.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                return string.Concat(hashBytes.Select(b => b.ToString("x2")));
            }
        }
    }

    internal class Program
    {
        // Globals
        static bool KafkaEnabled = false;
        static string KafkaBootstrapServers;
        static string KafkaTopic;
        static bool ElasticEnabled = false;
        static string ElasticServer;
        static string ElasticIndex;
        static string ElasticAPIKey;
        static string ElasticPassword;
        static string ElasticDataObjectName;
        static int IntervalMinutes;
        static string DataUrl;
        private static ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        static string ExecutingPath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
        
        // Enums
        enum Result
        {
            OK,
            Error,
            NoChange,
            Change
        }

        // Clients
        private static class ClientPool
        {
            public static ElasticsearchClient _elasticSearchClient = null;
            public static IProducer<Null, string> _kafkaProducer = null;
            public static IConsumer<Ignore, string> _kafkaConsumer = null;
        }
        static ElasticsearchClient _elasticSearchClient
        {
            get
            {
                if (ClientPool._elasticSearchClient == null)
                {
                    var settings = new ElasticsearchClientSettings(new Uri("http://" + ElasticServer)).Authentication(new ApiKey(ElasticAPIKey));
                    ClientPool._elasticSearchClient = new ElasticsearchClient(settings);
                }
                return ClientPool._elasticSearchClient;
            }
        }

        static IProducer<Null, string> _kafkaProducer
        {
            get
            {
                if (ClientPool._kafkaProducer == null)
                {
                    ProducerConfig _producerConfig = new ProducerConfig { BootstrapServers = KafkaBootstrapServers };
                    ClientPool._kafkaProducer = new ProducerBuilder<Null, string>(_producerConfig).SetErrorHandler(ProducerErrorHandler).SetLogHandler(ProducerLogHandler).Build();
                    
                }
                return ClientPool._kafkaProducer;
            }
        }

        static IConsumer<Ignore, string> _kafkaConsumer
        {
            get
            {
                if (ClientPool._kafkaConsumer == null)
                {
                    var _consumerConfig = new ConsumerConfig { BootstrapServers = KafkaBootstrapServers, GroupId = Guid.NewGuid().ToString(), EnableAutoCommit = false };
                    ClientPool._kafkaConsumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).SetErrorHandler(ConsumerErrorHandler).SetLogHandler(ConsumerLogHandler).Build();
                }
                return ClientPool._kafkaConsumer;
            }
        }

        // Config
        static Result ReadConfig()
        {
            try
            {
                var doc = XDocument.Load(Path.Combine(ExecutingPath, "config.xml"));
                // Kafka
                bool.TryParse(doc.Element("Config")?.Element("Kafka")?.Element("Enabled")?.Value, out KafkaEnabled);
                KafkaBootstrapServers = doc.Element("Config")?.Element("Kafka")?.Element("BootstrapServers")?.Value;
                KafkaTopic = doc.Element("Config")?.Element("Kafka")?.Element("Topic")?.Value;

                // Elastic
                bool.TryParse(doc.Element("Config")?.Element("Elastic")?.Element("Enabled")?.Value, out ElasticEnabled);
                ElasticServer = doc.Element("Config")?.Element("Elastic")?.Element("Server")?.Value;
                ElasticIndex = doc.Element("Config")?.Element("Elastic")?.Element("Index")?.Value;
                ElasticAPIKey = doc.Element("Config")?.Element("Elastic")?.Element("APIKey")?.Value;
                ElasticDataObjectName = doc.Element("Config")?.Element("Elastic")?.Element("DataObjectName")?.Value;

                // General / WEBAPI
                int.TryParse(doc.Element("Config")?.Element("IntervalMinutes")?.Value, out IntervalMinutes);
                DataUrl = doc.Element("Config")?.Element("DataUrl")?.Value;

                if (ElasticEnabled)
                    ConsoleAndLog($"PROG: Elastic Configuration [{ElasticServer}] [{ElasticIndex}]");

                if (KafkaEnabled)
                    ConsoleAndLog($"PROG: Kafka Configuration [{KafkaBootstrapServers}] [{KafkaTopic}]");
                
                if(!ElasticEnabled && !KafkaEnabled)
                {
                    ConsoleAndLog($"PROG: Please enable Elastic or Kafka.");
                    return Result.Error;
                }

                ConsoleAndLog($"PROG: WEBAPI Configuration [{DataUrl}] [{IntervalMinutes}m Interval]");
                return Result.OK;
            }
            catch (Exception ex)
            {
                ConsoleAndLog($"PROG: Error reading config from XML: {ex.Message}");
                return Result.Error;
            }
        }



        // Functions


        static void ConsoleAndLog(string text, bool logOnly = false)
        {
            string date = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            if(!logOnly)
                Console.WriteLine("[" + date + "] " + text);            
            File.AppendAllText(Path.Combine(ExecutingPath, "DevelopmentActivity.Producer.Log.txt"), "[" + date + "] " + text + Environment.NewLine);            
        }

        // Error Handling
        static void ConsumerErrorHandler(IConsumer<Ignore, string> consumer, Error error)
        {
            ConsoleAndLog($"KAFKA CON: Error [{error.Code}] [{error.Reason}]");
        }

        static void ProducerErrorHandler(IProducer<Null, string> producer, Error error)
        {
            ConsoleAndLog($"KAFKA PROD: Error [{error.Code}] [{error.Reason}]");
        }

        static void ConsumerLogHandler(IConsumer<Ignore, string> consumer, LogMessage log)
        {
            if (log.Level == SyslogLevel.Error)
            {
                ConsoleAndLog($"KAFKA CON: Error Message [{log.Message}]");
            }
            else if (log.Level == SyslogLevel.Warning)
            {
                ConsoleAndLog($"KAFKA CON: Warning Message [{log.Message}]", true);
            }
            else
            {
                ConsoleAndLog($"KAFKA CON: Log Message [{log.Message}]", true);
            }

            }

        static void ProducerLogHandler(IProducer<Null, string> producer, LogMessage log)
        {
            if (log.Level == SyslogLevel.Error)
            {
                ConsoleAndLog($"KAFKA PROD: Error Message [{log.Message}]");
            }
            else if (log.Level == SyslogLevel.Warning)
            {
                ConsoleAndLog($"KAFKA PROD: Warning Message [{log.Message}]", true);
            } else
            {
                ConsoleAndLog($"KAFKA PROD: Log Message [{log.Message}]", true);
            }
        }

        // Kafka
        static async Task<Result> ProduceToKafka(string jsonDataStr)
        {
            try
            {
                var deliveryResult = await _kafkaProducer.ProduceAsync(KafkaTopic, new Message<Null, string> { Value = jsonDataStr });                    
                ConsoleAndLog($"KAFKA PROD: Produced message to topic {deliveryResult.TopicPartitionOffset}");
                return Result.OK;            
            }
            catch (Exception ex)
            {
                ConsoleAndLog($"KAFKA PROD: Error [{ex.Message}]");
                if (ex is ProduceException<Null, string>)
                {
                    var e = ex as ProduceException<Null, string>;
                    ConsoleAndLog($"KAFKA PROD: Exception Code [{e.Error.Reason}] [{e.Error.Code}]");
                }
                return Result.Error;
            }
        }
        static async Task<Result> CompareToKafka(string jsonDataStr)
        {
            try
            {
                    // Seek to Latest Result
                    TopicPartition topicPartition = new(KafkaTopic, new Partition(0));
                    WatermarkOffsets watermarkOffsets = _kafkaConsumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(3));
                    TopicPartitionOffset topicPartitionOffset = new(topicPartition, new Offset(watermarkOffsets.High.Value - 1));
                    _kafkaConsumer.Assign(topicPartitionOffset);

                    // Fetch
                    var consumeResult = _kafkaConsumer.Consume(TimeSpan.FromSeconds(5)); // Adjust timeout as needed
                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        ConsoleAndLog("KAFKA CON: No messages found in the Kafka topic.");
                        return Result.Change; // No message in Kafka, consider it as different
                    }

                    var previousJsonData = consumeResult.Message.Value;
                    if(previousJsonData != null)
                    {
                        ConsoleAndLog($"KAFKA CON: Last message is {previousJsonData.Length} bytes [{previousJsonData.md5()}]");                    
                    }

                if (previousJsonData == jsonDataStr)
                {
                    ConsoleAndLog("KAFKA CON: Last data point is the same as the new data.");
                    return Result.NoChange;
                }

                ConsoleAndLog("KAFKA CON: Last data point is different from the new data.");
                return Result.Change;
            }
            catch (Exception ex)
            {
                ConsoleAndLog($"KAFKA CON: Error [{ex.Message}]");
                if (ex is ConsumeException)
                {
                    var e = ex as ConsumeException;
                    ConsoleAndLog($"KAFKA CON: Consume Exception [{e.Error.Reason}] [{e.Error.Code}]");                    
                }
                return Result.Error;
            }
        }
        
        // Elastic
        static async Task<Result> VerifyIndexElastic()
        {
            var indexExistsResponse = await _elasticSearchClient.Indices.ExistsAsync(ElasticIndex);

            if (indexExistsResponse.ApiCallDetails.HttpStatusCode != 404 && !indexExistsResponse.IsValidResponse) // If 404 it does not exist
            {
                ConsoleAndLog($"ELASTIC: Error Verifying Index [{ElasticIndex}] Exists [{indexExistsResponse.DebugInformation}");
                return Result.Error;
            }

            if (indexExistsResponse.ApiCallDetails.HttpStatusCode == 404 || !indexExistsResponse.Exists)
            {
                var indexResponse = await _elasticSearchClient.Indices.CreateAsync(ElasticIndex);
                if (!indexExistsResponse.IsValidResponse)
                {
                    ConsoleAndLog($"ELASTIC: Created Index [{ElasticIndex}]");
                }
                else
                {
                    ConsoleAndLog($"ELASTIC: Error Creating Index [{ElasticIndex}] [{indexResponse.DebugInformation}");
                    return Result.Error;
                }
            }
            else
            {
                ConsoleAndLog($"ELASTIC: Verified Index [{ElasticIndex}] Exists");
            }
            return Result.OK;
        }

        static async Task<Result> CompareToElastic(string jsonDataStr)
        {
            try
            {
                // See if any records exist
                var countDataResponse = await _elasticSearchClient.CountAsync(ElasticIndex);
                if (!countDataResponse.IsValidResponse)
                {
                    ConsoleAndLog($"ELASTIC: Error unable to determine record count [{countDataResponse.DebugInformation}]");
                    return Result.Error;
                } else
                {
                    var count = countDataResponse.Count;
                    ConsoleAndLog($"ELASTIC: [{ElasticIndex}] contains [{count}] records");
                    if (count == 0)
                        return Result.Change;
                }

                // Retrieve the last data point from Elasticsearch
                var lastDataResponse = await _elasticSearchClient.SearchAsync<JsonNode>(s => s
                    .Index(ElasticIndex)
                    .Size(1)  // Retrieve only the last document
                    .Sort(sort => sort.Field("timestamp"))  // Sort by descending document ID to get the last document
                );

                if (!lastDataResponse.IsValidResponse)
                {
                    ConsoleAndLog($"ELASTIC: Error retrieving last data point. Error: [{lastDataResponse.DebugInformation}]");
                    return Result.Error;
                }

                // Check if the last data point is different from the new data
                var previousJsonData = lastDataResponse.Documents.FirstOrDefault();
                ConsoleAndLog($"ELASTIC: Last message has hash [{previousJsonData["md5"].ToString()}]");

                if (previousJsonData != null && previousJsonData["md5"].ToString() == jsonDataStr.md5())
                {
                    ConsoleAndLog("ELASTIC: Last data point is the same as the new data.");
                    return Result.NoChange;
                }

                ConsoleAndLog("ELASTIC: Last data point is different from the new data.");
                return Result.Change;
            }
            catch (Exception ex)
            {
                ConsoleAndLog($"ELASTIC: Unable to compare data with Elasticsearch. Error: [{ex.Message}]");
                return Result.Error;
            }
        }


        static async Task<Result> ProduceToElastic(string jsonDataStr)
        {        
            var id = Guid.NewGuid().ToString();
            try
            {                
                var wrappedData = new
                {
                    md5 = jsonDataStr.md5(), // Calculate MD5 hash of the original data
                    timestamp = DateTime.Now.ToFileTime(), // Timestamp
                    data = JsonNode.Parse(jsonDataStr) // Original Data
                };

                var dataResponse = await _elasticSearchClient.CreateAsync(wrappedData, ElasticIndex, id);
                if (!dataResponse.IsValidResponse)
                {
                    ConsoleAndLog($"ELASTIC: Error Sending Data [{dataResponse.DebugInformation}");
                    return Result.Error;                    
                } else
                {
                    ConsoleAndLog($"ELASTIC: Successfully Sent Data to Elastic Instance");
                }

            } catch(Exception ex)
            {
                ConsoleAndLog($"ELASTIC: Unable to Index to ElasticSearch. Error [{ex.Message}]");
                return Result.Error;
            }
            ConsoleAndLog($"ELASTIC: Indexed to ElasticSearch as ID [{id}]");
            return Result.OK;
        }

        static async Task FetchAndSendData()
        {
            try
            {
                using (var webClient = new WebClient())
                {
                    var downloadCompleted = new TaskCompletionSource<bool>();
                    var jsonData = new StringBuilder();

                    webClient.DownloadProgressChanged += (sender, args) =>
                    {
                        UpdateProgressBar(args.ProgressPercentage);                        
                    };

                    webClient.DownloadStringCompleted += (sender, args) =>
                    {
                        if (args.Error != null)
                        {
                            ConsoleAndLog($"WEBAPI: Error while fetching data: {args.Error.Message}");
                        }
                        else
                        {
                            Console.WriteLine(""); // Skip after progress bar
                            ConsoleAndLog("WEBAPI: Download Complete");
                            jsonData.Append(args.Result);
                            downloadCompleted.SetResult(true);
                        }
                    };

                    webClient.DownloadStringAsync(new Uri(DataUrl));

                    // Wait for download to complete or timeout
                    await Task.WhenAny(downloadCompleted.Task, Task.Delay(TimeSpan.FromMinutes(5)));

                    if (!downloadCompleted.Task.IsCompleted)
                    {
                        webClient.CancelAsync();
                        ConsoleAndLog("WEBAPI: Download timed out.");
                        return;
                    }
                    var jsonDataStr = jsonData.ToString();
 
                    ConsoleAndLog($"PROG: New data is {jsonDataStr.Length} bytes [{jsonDataStr.md5()}]");

                    if (ElasticEnabled)
                    {
                        ConsoleAndLog("PROG: Comparing to Last Elastic Datapoint");
                        var elasticIndexResult = await VerifyIndexElastic();
                        if (elasticIndexResult == Result.OK)
                        {
                            var elasticCompareResult = await CompareToElastic(jsonDataStr);

                            if (elasticCompareResult == Result.Change)
                            {
                                ConsoleAndLog("PROG: Data has changed, producing new data to Elastic");
                                await ProduceToElastic(jsonDataStr);
                            }
                            else if (elasticCompareResult == Result.Error)
                            {
                                ConsoleAndLog("PROG: Error when comparing data, no action taken");
                            }
                        }
                    }
                    if (KafkaEnabled)
                    {
                        ConsoleAndLog("PROG: Comparing to Last Kafka Datapoint");
                        var kafkaCompareResult = await CompareToKafka(jsonDataStr);

                        if (kafkaCompareResult == Result.Change)
                        {
                            ConsoleAndLog("PROG: Data has changed, producing new data to Kafka");
                            await ProduceToKafka(jsonDataStr);
                        }
                        else if (kafkaCompareResult == Result.Error)
                        {
                            ConsoleAndLog("PROG: Error when comparing data, no action taken");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ConsoleAndLog($"PROG: Error {ex.Message}");
            }
        }

        static bool _updatingProgressBar = false;
        static async void UpdateProgressBar(double percent)
        {
            if (_updatingProgressBar)
                return;
            _updatingProgressBar = true;
            // Update the progress bar
            int barLength = (int)Math.Round(percent / 2); // 50 characters for 100%
            Console.Write("[" + new string('=', barLength) + new string(' ', 50 - barLength) + "]");
            Console.Write(" " + percent + "%");            
            Console.Write("\r"); // Move the cursor to the beginning of the line
            _updatingProgressBar = false;
        }

        static void Main(string[] args)
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║        [DevelopmentActivity.Producer] Module      ║");
            Console.WriteLine("║                                                   ║");
            Console.WriteLine("║                Strong Towns Langley               ║");
            Console.WriteLine("║            DevelopmentActivity Analytics          ║");            
            Console.WriteLine("╚═══════════════════════════════════════════════════╝");

            ConsoleAndLog("PROG: Program starting");
            if(ReadConfig() == Result.Error)
            {
                ConsoleAndLog("PROG: Exiting...");
                return;
            }

            var timer = new Timer(async _ =>
            {
                ConsoleAndLog("WEBAPI: Updating data");
                await FetchAndSendData();
                ConsoleAndLog($"PROG: Pausing for {IntervalMinutes} minutes");
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(IntervalMinutes));

            _waitHandle.Wait(); // Wait indefinitely, keeping the main thread alive
        }
    }
}