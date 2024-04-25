using Confluent.Kafka;
using DevelopmentActivity.Producer.Interfaces;
using DevelopmentActivity.Producer.Shared;
using System;
using System.Threading.Tasks;

namespace DevelopmentActivity.Producer.Kafka
{
    public class DataService : IDataService
    {
        public async Task<RequestResult> ProduceToDataStore(string jsonDataStr)
        {
            try
            {
                var producer = Kafka.ClientFactory.GetProducer(Config.BootstrapServers);
                var deliveryResult = await producer.ProduceAsync(Config.Topic, new Message<Null, string> { Value = jsonDataStr });
                Logger.Log($"KAFKA PROD", $"Produced message to topic [{Config.Topic}]");
                return RequestResult.OK;
            }
            catch (Exception ex)
            {
                Logger.Log($"KAFKA PROD", $"Error producing to Kafka: {ex.Message}");
                return RequestResult.Error;
            }
        }

        public async Task<RequestResult> CompareToDataStore(string jsonDataStr)
        {
            try
            {
                var consumer = Kafka.ClientFactory.GetConsumer(Config.BootstrapServers, Guid.NewGuid().ToString());

                // Seek to Latest Result
                TopicPartition topicPartition = new(Config.Topic, new Partition(0));
                WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(3));
                TopicPartitionOffset topicPartitionOffset = new(topicPartition, new Offset(watermarkOffsets.High.Value - 1));
                consumer.Assign(topicPartitionOffset);

                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));
                
                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    Logger.Log("KAFKA CON",$"No messages found in the Kafka topic.");
                    return RequestResult.Change; // No message in Kafka, consider it as different
                }

                var previousJsonData = consumeResult.Message.Value;               
                if (previousJsonData != null)
                {
                    Logger.Log("KAFKA CON", $"Last message is {previousJsonData.Length} bytes [{previousJsonData.md5()}]");
                }

                if (previousJsonData != null && previousJsonData.md5() == jsonDataStr.md5())
                {
                    Logger.Log("KAFKA CON",$"Last data point is the same as the new data.");
                    return RequestResult.NoChange;
                }

                Logger.Log("KAFKA CON",$"Last data point is different from the new data.");
                return RequestResult.Change;
            }
            catch (Exception ex)
            {
                Logger.Log($"KAFKA PROD", $"Produced message to topic {ex.Message}");
                return RequestResult.Error;
            }
        }

        public Task<string> FetchData(string request)
        {
            throw new NotImplementedException();
        }

        public Task<RequestResult> VerifyContainerExists(string container)
        {
            throw new NotImplementedException();
        }
    }
}
