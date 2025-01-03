using Microsoft.Azure.Amqp;
using System;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace AzureEventHubClient
{
    public class EventHubConfig
    {
        public required string EventHubName { get; set; }
        public required string EventHubNameSpace { get; set; }
        public required string EventHubConnectionString { get; set; }
        public required string StorageConnectionString { get; set; }
        public required string BlobContainerName { get; set; }

    }

    public class AppConfig
    {
        public required EventHubConfig EventHubConfig { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            //Read Event Hub Config file
            AppConfig appConfig = PopulateAppConfig();

           //Send Events
           EventSender eventSender = new EventSender(
               appConfig.EventHubConfig.EventHubNameSpace,
               appConfig.EventHubConfig.EventHubName,
               appConfig.EventHubConfig.EventHubConnectionString);
            
           eventSender.SendEvents().Wait();
            
            
            
            //Process Events

            EventReceiver eventReceiver = new EventReceiver(
                appConfig.EventHubConfig.EventHubNameSpace, 
                appConfig.EventHubConfig.EventHubName,
                appConfig.EventHubConfig.EventHubConnectionString,
                appConfig.EventHubConfig.StorageConnectionString, 
                appConfig.EventHubConfig.BlobContainerName);

            eventReceiver.ProcessEvents().Wait();            
        }

        private static AppConfig PopulateAppConfig()
        {
            // Path to the YAML file
            string configFile = "config.yaml";
            // Read the file content
            string configContent = File.ReadAllText(configFile);

            // Create a deserializer with camel case naming convention
            var deserializer = new DeserializerBuilder()               
                .Build();

            // Deserialize the YAML content into the Config object
            var config = deserializer.Deserialize<AppConfig>(configContent);
            return config;
        }
    }
}