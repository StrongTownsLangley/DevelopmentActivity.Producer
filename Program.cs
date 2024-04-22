﻿using Confluent.Kafka;
using System;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using System.Reflection;

namespace PermitActivity.Producer
{
    internal class Program
    {
        private const string KafkaBootstrapServers = "66.94.126.17:9092";
        private const string KafkaTopic = "DevelopmentActivity";
        private const string KafkaGroupId = "default";
        private static ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);
        private static ProducerConfig _producerConfig = new ProducerConfig { BootstrapServers = KafkaBootstrapServers };

        // https://data-tol.opendata.arcgis.com/datasets/TOL::development-activity-status-table/about
        private const string DataUrl = "https://services5.arcgis.com/frpHL0Fv8koQRVWY/arcgis/rest/services/Development_Activity_Status_Table/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson";
        private const int IntervalMinutes = 30;

        enum Result
        {
            OK,
            Error,
            NoChange,
            Change
        }

        static void ConsoleAndLog(string text, bool logOnly = false)
        {
            string date = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            if(!logOnly)
                Console.WriteLine("[" + date + "] " + text);
            var path = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            File.AppendAllText(Path.Combine(path, "DevelopmentActivity.Producer.Log.txt"), "[" + date + "] " + text + Environment.NewLine);
            
        }

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
            if(log.Level == SyslogLevel.Error)
            {
                ConsoleAndLog($"KAFKA CON: Error Message [{log.Message}]");
            } else
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
            else
            {
                ConsoleAndLog($"KAFKA PROD: Log Message [{log.Message}]", true);
            }
        }

        static async Task<Result> ProduceToKafka(string data)
        {
            try
            {                
                using (var producer = new ProducerBuilder<Null, string>(_producerConfig).SetErrorHandler(ProducerErrorHandler).SetLogHandler(ProducerLogHandler).Build())
                {
                    var deliveryResult = await producer.ProduceAsync(KafkaTopic, new Message<Null, string> { Value = data });                    
                    ConsoleAndLog($"KAFKA PROD: Produced message '{deliveryResult.Value}' to topic {deliveryResult.TopicPartitionOffset}");
                    return Result.OK;
                }
  
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
        static async Task<Result> CompareToKafka(string newData)
        {
            try
            {
                var _consumerConfig = new ConsumerConfig { BootstrapServers = KafkaBootstrapServers, GroupId = Guid.NewGuid().ToString(), EnableAutoCommit = false };
                using (var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).SetErrorHandler(ConsumerErrorHandler).SetLogHandler(ConsumerLogHandler).Build())
                {
                    // Seek to Latest Result
                    TopicPartition topicPartition = new(KafkaTopic, new Partition(0));
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(3));
                    TopicPartitionOffset topicPartitionOffset = new(topicPartition, new Offset(watermarkOffsets.High.Value - 1));
                    consumer.Assign(topicPartitionOffset);

                    // Fetch
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5)); // Adjust timeout as needed
                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        ConsoleAndLog("KAFKA CON: No messages found in the Kafka topic.");
                        return Result.Change; // No message in Kafka, consider it as different
                    }

                    var lastMessage = consumeResult.Message.Value;
                    if(lastMessage != null)
                    {
                        ConsoleAndLog("KAFKA CON: Last message is " + lastMessage.Length + " bytes");
                    }

                    return newData != lastMessage ? Result.Change : Result.NoChange;
                }
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
                    var newData = jsonData.ToString();
                    ConsoleAndLog("PROG: New data is " + newData.Length + " bytes");
                    ConsoleAndLog("PROG: Comparing to Last Kafka Datapoint");

                    var compareResult = await CompareToKafka(newData);

                    if (compareResult == Result.Change)
                    {
                        ConsoleAndLog("PROG: Data has changed, producing new data to Kafka");
                        await ProduceToKafka(newData);
                    } else if (compareResult == Result.NoChange)
                    {
                        ConsoleAndLog("PROG: No change to data, no action taken");
                    } else if (compareResult == Result.Error)
                    {
                        ConsoleAndLog("PROG: Error when comparing data, no action taken");                        
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
            Console.WriteLine("║           Development Activity Analytics          ║");
            Console.WriteLine("║                 for Apache Kafka                  ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝");


            var timer = new Timer(async _ =>
            {
                ConsoleAndLog("WEBAPI: Updating data at " + DateTime.Now.ToString());
                await FetchAndSendData();
                ConsoleAndLog($"PROG: Pausing for {IntervalMinutes} minutes");
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(IntervalMinutes));

            _waitHandle.Wait(); // Wait indefinitely, keeping the main thread alive
        }
    }
}