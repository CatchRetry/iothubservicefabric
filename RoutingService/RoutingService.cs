using System;
using System.Collections.Generic;
using System.Fabric;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace RoutingService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class RoutingService : StatefulService
    {
        private const string OffsetDictionaryName = "OffsetDictionary";
        private const string OffsetKey = "offsetKey";
        private const int OffsetMsgInterval = 10;

        public RoutingService(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            var partitionKey = GetServicePartitionKey();
            // This service message can be seen by adding 'MyCompany-IotIngestion-RoutingService' in Diagnostic Event view / Configure
            ServiceEventSource.Current.ServiceMessage(this.Context, $"ServiceContext started for Partition {partitionKey}");

            string iotHubConnectionString = GetIotHubConnectionString();
            ServiceEventSource.Current.ServiceMessage(this.Context, $"IotHub ConnectionString = {iotHubConnectionString}");

            // Get an EventHub client connected to the IOT Hub
            EventHubClient eventHubClient = GetAmqpEventHubClient(iotHubConnectionString);

            // These Reliable Dictionaries are used to keep track of our position in IoT Hub.
            // If this service fails over, this will allow it to pick up where it left off in the event stream.
            IReliableDictionary<string, string> offsetDictionary =
                await this.StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetDictionaryName);

            // Get a receiver connected to the service matching partition of the IOT Hub
            EventHubReceiver eventHubReceiver = await GetEventHubReceiverAsync(eventHubClient, partitionKey, offsetDictionary);

            int readMsgCountSinceOffset = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    using (EventData eventData = await eventHubReceiver.ReceiveAsync(TimeSpan.FromSeconds(5)))
                    {
                        if (eventData == null)
                        {
                            continue;
                        }

                        ServiceEventSource.Current.Message(
                            $"(RoutingService's Partition {partitionKey}) received a message: {Encoding.UTF8.GetString(eventData.GetBytes())})"
                        );

                        if (++readMsgCountSinceOffset == OffsetMsgInterval-1)
                        {
                            using (ITransaction tx = this.StateManager.CreateTransaction())
                            {
                                await offsetDictionary.SetAsync(tx, OffsetKey, eventData.Offset);
                                await tx.CommitAsync();
                            }
                            readMsgCountSinceOffset = 0;
                        }
                    }
                }
                catch (Exception e)
                {
                    ServiceEventSource.Current.ServiceMessage(
                        this.Context,
                        "{0}",
                        new[] { e.Message });
                }
            }
        }

        /// <summary>
        /// Get the IoT Hub connection string from the Settings.xml config file
        /// from a configuration package named "Config"
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        private string GetIotHubConnectionString()
        {
            return this.Context.CodePackageActivationContext
                .GetConfigurationPackageObject("Config")
                .Settings
                .Sections["IoTHubConfigInformation"]
                .Parameters["ConnectionString"]
                .Value;
        }

        /// <summary>
        /// Each partition of this service corresponds to a partition in IoT Hub.
        /// IoT Hub partitions are numbered 0..n-1, up to n = 32.
        /// This service needs to use an identical partitioning scheme. 
        /// The low key of every partition corresponds to an IoT Hub partition.
        /// </summary>
        /// <returns></returns>
        private long GetServicePartitionKey()
        {
            Int64RangePartitionInformation partitionInfo = (Int64RangePartitionInformation)this.Partition.PartitionInfo;
            long servicePartitionKey = partitionInfo.LowKey;
            return servicePartitionKey;
        }

        /// <summary>
        /// Creates an EventHubClient through AMQP protocol
        /// </summary>
        /// <param name="iotHubConnectionString"></param>
        private EventHubClient GetAmqpEventHubClient(string iotHubConnectionString)
        {
            // EventHubs doesn't support NetMessaging, so ensure the transport type is AMQP.
            ServiceBusConnectionStringBuilder connectionStringBuilder =
                new ServiceBusConnectionStringBuilder(iotHubConnectionString)
                {
                    TransportType = TransportType.Amqp
                };

            ServiceEventSource.Current.ServiceMessage(
                this.Context,
                "RoutingService connecting to IoT Hub at {0}",
                new object[] { string.Join(",", connectionStringBuilder.Endpoints.Select(x => x.ToString())) });

            // A new MessagingFactory is created here so that each partition of this service will have its own MessagingFactory.
            // This gives each partition its own dedicated TCP connection to IoT Hub.
            MessagingFactory messagingFactory = MessagingFactory.CreateFromConnectionString(connectionStringBuilder.ToString());
            EventHubClient eventHubClient = messagingFactory.CreateEventHubClient("messages/events");

            return eventHubClient;
        }

        /// <summary>
        /// Creates an EventHubReceiver connected though ServiceBus
        /// with AMQP protocol
        /// </summary>
        /// <param name="eventHubClient">Event Hub Client</param>
        /// <param name="partitionKey">IOT Hub Partition Key</param>
        /// <param name="offsetDictionary"></param>
        /// <returns></returns>
        private async Task<EventHubReceiver> GetEventHubReceiverAsync(EventHubClient eventHubClient, long partitionKey, IReliableDictionary<string, string> offsetDictionary)
        {
            //Get the EventHubRuntimeInfo
            EventHubRuntimeInformation eventHubRuntimeInfo = await eventHubClient.GetRuntimeInformationAsync();

            // Get an IoT Hub partition ID that corresponds to this partition's low key.
            // This assumes that this service has a partition count 'n' that is equal to the IoT Hub partition count and a partition range of 0..n-1.
            // For example, given an IoT Hub with 32 partitions, this service should be created with:
            // partition count = 32
            // partition range = 0..31
            string eventHubPartitionId = eventHubRuntimeInfo.PartitionIds[partitionKey];

            EventHubReceiver eventHubReceiver;

            using (ITransaction tx = this.StateManager.CreateTransaction())
            {
                ConditionalValue<string> offsetResult =
                    await offsetDictionary.TryGetValueAsync(tx, OffsetKey, LockMode.Default);

                if (offsetResult.HasValue)
                {
                    ServiceEventSource.Current.ServiceMessage(
                        this.Context,
                        $"RoutingService partition Key {partitionKey} connecting to IoT Hub partition ID {eventHubPartitionId} with offset {offsetResult.Value}");

                    eventHubReceiver = await eventHubClient.GetDefaultConsumerGroup()
                        .CreateReceiverAsync(eventHubPartitionId, offsetResult.Value);
                }
                else
                {
                    var dateTimeUtc = DateTime.UtcNow;
                    ServiceEventSource.Current.ServiceMessage(
                        this.Context,
                        $"RoutingService partition Key {partitionKey} connecting to IoT Hub partition ID {eventHubPartitionId} with offset {dateTimeUtc}");

                    eventHubReceiver = await eventHubClient.GetDefaultConsumerGroup()
                        .CreateReceiverAsync(eventHubPartitionId, dateTimeUtc);
                }
            }
            return eventHubReceiver;
        }
    }
}
