using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;

namespace AzureEventHubClient
{

    class Event
    {
        public int EventId { get; set; }
        public required string EventName { get; set; }
        public required string EventDate { get; set; }
        public required string Location { get; set; }
        public int Attendees { get; set; }
    }

    class EventSender
    {
        string EventHubName;
        string EventHubNameSpace;
        string ConnectionString;
        TokenCredential Credential;
        EventHubProducerClient ProducerClient;

        public EventSender(string eventHubNameSpace, string eventHubName, string connectionString)
        {
            EventHubName = eventHubName;
            EventHubNameSpace = eventHubNameSpace;
            ConnectionString = connectionString;
            Credential = new DefaultAzureCredential();
            ProducerClient = new EventHubProducerClient(connectionString, eventHubName);
        }


        // Populate events
        public List<EventData> PopulateEvents()
        {
            try
            {
                string eventJsonContent = File.ReadAllText("SampleData.json");
                List<Event> events = JsonConvert.DeserializeObject<List<Event>>(eventJsonContent);
                if (events == null || events.Count == 0)
                {
                    throw new Exception("No event read from SampleData.json");
                }

                EventData data;
                List<EventData> eventDataList = new List<EventData>();
                foreach (Event e in events)
                {
                    data = new EventData();
                    data.EventBody = new BinaryData(e);
                    eventDataList.Add(data);
                }
                return eventDataList;
            }
            catch (Exception ex)
            {
                throw new Exception($"Could not generate event array from json file.{ex}");
            }
        }


        //published events
        public async Task SendEvents()
        {

            List<EventData> eventDataList = PopulateEvents();

            try
            {
                int i = 0;
                using EventDataBatch eventBatch = await ProducerClient.CreateBatchAsync();
                foreach (var eventData in eventDataList)
                {
                    if (!eventBatch.TryAdd(eventData))
                    {
                        // At this point, the batch is full but our last event was not
                        // accepted.  For our purposes, the event is unimportant so we
                        // will intentionally ignore it.  In a real-world scenario, a
                        // decision would have to be made as to whether the event should
                        // be dropped or published on its own.
                        Console.WriteLine("Not able to add anymore batch.");
                        break;
                    }
                    i += 1;
                }

                // When the producer publishes the event, it will receive an
                // acknowledgment from the Event Hubs service; so long as there is no
                // exception thrown by this call, the service assumes responsibility for
                // delivery.  Your event data will be published to one of the Event Hub
                // partitions, though there may be a (very) slight delay until it is
                // available to be consumed.
                Console.WriteLine($"{i} event sent in {EventHubName}");
                await ProducerClient.SendAsync(eventBatch);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in sent events-{e}");
                // Transient failures will be automatically retried as part of the
                // operation. If this block is invoked, then the exception was either
                // fatal or all retries were exhausted without a successful publish.
            }
            finally
            {
                await ProducerClient.CloseAsync();
            }
        }
    }
}
