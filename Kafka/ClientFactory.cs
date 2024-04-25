using Confluent.Kafka;
using System;
using DevelopmentActivity.Producer.Shared;

namespace DevelopmentActivity.Producer.Kafka
{
    public static class ClientFactory
    {
        private static IProducer<Null, string> _kafkaProducer;
        private static IConsumer<Ignore, string> _kafkaConsumer;

        public static IProducer<Null, string> GetProducer(string bootstrapServers)
        {
            if (_kafkaProducer == null)
            {
                var config = new ProducerConfig { BootstrapServers = bootstrapServers };
                _kafkaProducer = new ProducerBuilder<Null, string>(config)
                                 .SetErrorHandler((_, e) => Logger.Log("KAFKA PROD", $"Error [{e.Reason}]"))
                                 .SetLogHandler((_, log) => Logger.Log("KAFKA PROD", log))
                                 .Build();
            }
            return _kafkaProducer;
        }

        public static IConsumer<Ignore, string> GetConsumer(string bootstrapServers, string groupId)
        {
            if (_kafkaConsumer == null)
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
                _kafkaConsumer = new ConsumerBuilder<Ignore, string>(config)
                                 .SetErrorHandler((_, e) => Logger.Log("KAFKA CON", $"Error [{e.Reason}]"))
                                 .SetLogHandler((_, log) => Logger.Log("KAFKA CON", log))
                                 .Build();
            }
            return _kafkaConsumer;
        }
    }
}
