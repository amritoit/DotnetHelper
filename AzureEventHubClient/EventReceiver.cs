using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace AzureEventHubClient
{

    class EventReceiver
    {
        public static int eventCount = 0;
        string EventHubName;
        string EventHubNameSpace;
        string EHConnectionString;
        TokenCredential Credential;
        EventHubProducerClient ProducerClient;
        string StorageConnectionString;
        string BlobContainerName;


        public EventReceiver(
            string eventHubNameSpace,
            string eventHubName,
            string ehConnectionString,
            string storageConnectionString,
            string blobContainerName)
        {
            EventHubName = eventHubName;
            EventHubNameSpace = eventHubNameSpace;
            EHConnectionString = ehConnectionString;
            Credential = new DefaultAzureCredential();
            ProducerClient = new EventHubProducerClient(ehConnectionString, eventHubName);
            StorageConnectionString = storageConnectionString;
            BlobContainerName = blobContainerName;
        }


        public async Task ProcessEvents()
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
            var storageClient = new BlobContainerClient(
                StorageConnectionString,
                BlobContainerName);

            var processor = new EventProcessorClient(
                storageClient,
                consumerGroup,
                EHConnectionString,
                EventHubName);


            try
            {
                using var cancellationSource = new CancellationTokenSource();
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));

                processor.ProcessEventAsync += ProcessEventHandler;
                processor.ProcessErrorAsync += ProcessErrorHandler;

                try
                {
                    await processor.StartProcessingAsync(cancellationSource.Token);
                    await Task.Delay(Timeout.Infinite, cancellationSource.Token);
                }
                catch (TaskCanceledException)
                {                   
                    Console.WriteLine($"Task is cancelled by caller.");
                }
                finally
                {
                    await processor.StopProcessingAsync();
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Temporary exception in Event processor.-{ex}");
            }
            finally
            {
                processor.ProcessEventAsync -= ProcessEventHandler;
                processor.ProcessErrorAsync -= ProcessErrorHandler;
            }
        }

        async Task ProcessEventHandler(ProcessEventArgs args)
        {
            try
            {
                eventCount++;
                var partitionEventCount = new ConcurrentDictionary<string, int>();

                // If the cancellation token is signaled, then the
                // processor has been asked to stop.  It will invoke
                // this handler with any events that were in flight;
                // these will not be lost if not processed.
                //
                // It is up to the handler to decide whether to take
                // action to process the event or to cancel immediately.

                if (args.CancellationToken.IsCancellationRequested)
                {
                    return;
                }

                string partition = args.Partition.PartitionId;
                byte[] eventBody = args.Data.EventBody.ToArray();
                Console.WriteLine($"Event-{eventCount} from partition {partition} with length {eventBody.Length}.");

                int eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
                    key: partition,
                    addValue: 1,
                    updateValueFactory: (_, currentCount) => currentCount + 1);

                if (eventsSinceLastCheckpoint >= 50)
                {
                    await args.UpdateCheckpointAsync();
                    partitionEventCount[partition] = 0;
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception in event process handler-{ex}.");
            }
        }


        Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            try
            {
                Debug.WriteLine("Error in the EventProcessorClient");
                Debug.WriteLine($"\tOperation: {args.Operation}");
                Debug.WriteLine($"\tException: {args.Exception}");
                Debug.WriteLine("");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception in event process error handler-{ex}.");
            }
            return Task.CompletedTask;
        }
    }
}
