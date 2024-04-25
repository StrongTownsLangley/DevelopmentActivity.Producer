namespace DevelopmentActivity.Producer.Kafka
{
    public class Config
    {
        public static bool Enabled { get; set; }
        public static string BootstrapServers { get; set; }
        public static string Topic { get; set; }
    }
}