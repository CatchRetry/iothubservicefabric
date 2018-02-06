using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
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

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
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
    }
}
