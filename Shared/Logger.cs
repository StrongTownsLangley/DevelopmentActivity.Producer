using Confluent.Kafka;
using System;
using System.IO;
using System.Reflection;

namespace DevelopmentActivity.Producer.Shared
{
    public static class Logger
    {
        private static readonly string LogFilePath = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "DevelopmentActivity.Producer.Log.txt");

        public static void Log(string source, string message, bool logOnly = false)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string logEntry = $"[{timestamp}] {source}: {message}";

            if (!logOnly)
                Console.WriteLine(logEntry);

            File.AppendAllText(LogFilePath, logEntry + Environment.NewLine);
        }

        public static void Log(string source, LogMessage message)
        {
            if (message.Level >= SyslogLevel.Warning)
            {
                Logger.Log(source, message.Message);
            }
            else
            {
                Logger.Log(source, message.Message, true);
            }
        }
    }
}
