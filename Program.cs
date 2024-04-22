using Confluent.Kafka;
using System;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;

namespace PermitActivity.Producer
{
    internal class Program
    {
        private const string KafkaBootstrapServers = "localhost:9092";
        private const string Topic = "DevelopmentActivity";
        private static ManualResetEventSlim _waitHandle = new ManualResetEventSlim(false);


        // https://data-tol.opendata.arcgis.com/datasets/TOL::development-activity-status-table/about
        private const string DataUrl = "https://services5.arcgis.com/frpHL0Fv8koQRVWY/arcgis/rest/services/Development_Activity_Status_Table/FeatureServer/1/query?outFields=*&where=1%3D1&f=geojson";
        private const int IntervalMinutes = 30;

        static async Task ProduceToKafka(string data)
        {
            try
            {
                var config = new ProducerConfig { BootstrapServers = KafkaBootstrapServers };
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var deliveryResult = await producer.ProduceAsync(Topic, new Message<Null, string> { Value = data });                    
                    Console.WriteLine($"4. Produced message '{deliveryResult.Value}' to topic {deliveryResult.TopicPartitionOffset}");
                }
  
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
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
                            Console.WriteLine($"Error while fetching data: {args.Error.Message}");
                        }
                        else
                        {
                            Console.WriteLine("");
                            Console.WriteLine("2. Download Complete");
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
                        Console.WriteLine("Download timed out.");
                        return;
                    }

                    Console.WriteLine("3. Sending to Kafka");
                    await ProduceToKafka(jsonData.ToString());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while fetching data: {ex.Message}");
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
                Console.WriteLine("1. Updating data at " + DateTime.Now.ToString());
                await FetchAndSendData();
                Console.WriteLine($"5. Pausing for {IntervalMinutes} minutes");
            }, null, TimeSpan.Zero, TimeSpan.FromMinutes(IntervalMinutes));

            _waitHandle.Wait(); // Wait indefinitely, keeping the main thread alive
        }
    }
}