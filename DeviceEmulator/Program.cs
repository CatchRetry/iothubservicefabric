using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

namespace DeviceEmulator
{
    class Program
    {
        private static string _iotHubConnectionString;
        private static RegistryManager _registryManager;
        private static int _messageId = 0;

        static void Main(string[] args)
        {
            Console.WriteLine("Enter IoT Hub connection string: ");
            _iotHubConnectionString = Console.ReadLine();

            _registryManager = RegistryManager.CreateFromConnectionString(_iotHubConnectionString);

            Task.Run(
                async () =>
                {
                    while (true)
                    {
                        try
                        {
                            Console.WriteLine();
                            Console.WriteLine("Commands:");
                            Console.WriteLine("1: Register random devices");
                            Console.WriteLine("2: Send data from all devices");
                            Console.WriteLine("3: Remove all devices");
                            Console.WriteLine("4: Exit");

                            string command = Console.ReadLine();

                            switch (command)
                            {
                                case "1":
                                    Console.WriteLine("Number of devices?");
                                    int num = Int32.Parse(Console.ReadLine());
                                    await AddRandomDevicesAsync(num);
                                    break;
                                case "2":
                                    Console.WriteLine("Number of messages?");
                                    int nbMessages = Int32.Parse(Console.ReadLine());
                                    await SendFromAllDevicesAsync(nbMessages);
                                    break;
                                case "3":
                                    Console.WriteLine("Do you confirm? (y/n)");
                                    var confirm = Console.ReadLine();
                                    if (confirm != null && confirm.ToLower().Equals("y"))
                                    {
                                        var devices = await _registryManager.GetDevicesAsync(Int32.MaxValue);
                                        //var query = registryManager.CreateQuery("select * from devices");
                                        //while (query.HasMoreResults)
                                        //{
                                        //    var page = await query.GetNextAsTwinAsync();
                                        //    foreach (var twin in page)
                                        //    {
                                        //        // do work on twin object
                                        //        devices.Append(twin.)
                                        //    }
                                        //}
                                        foreach (var device in devices)
                                        {
                                            await _registryManager.RemoveDeviceAsync(device.Id);
                                        }
                                    }
                                    break;
                                case "4":
                                    return;
                                default:
                                    break;
                            }

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("An error occured: {0}", ex.Message);
                        }
                    }
                }
            ).GetAwaiter().GetResult();
        }

        private static async Task AddRandomDevicesAsync(int count)
        {
            for (int i = 0; i < count; i++)
            {
                await AddDeviceAsync("device#" + Guid.NewGuid());
            }
        }

        private static async Task AddDeviceAsync(string deviceId)
        {
            try
            {
                await _registryManager.AddDeviceAsync(new Device(deviceId));
                Console.WriteLine("Added device {0}", deviceId);
            }
            catch (Microsoft.Azure.Devices.Common.Exceptions.DeviceAlreadyExistsException)
            {
            }
        }

        private static async Task SendFromAllDevicesAsync(int nbMessages)
        {
            string iotHubUri = _iotHubConnectionString.Split(';')
                .First(x => x.StartsWith("HostName=", StringComparison.InvariantCultureIgnoreCase))
                .Replace("HostName=", "").Trim();

            var devices = await _registryManager.GetDevicesAsync(Int32.MaxValue);
            double minTemperature = 20;
            double minHumidity = 60;
            Random rand = new Random();

            var tasks = new List<Task>();
            for (int i = 0; i < nbMessages; i++)
            {
                foreach (Device device in devices)
                {
                    double currentTemperature = minTemperature + rand.NextDouble() * 15;
                    double currentHumidity = minHumidity + rand.NextDouble() * 20;

                    var telemetryDataPoint = new
                    {
                        messageId = _messageId++,
                        deviceId = device.Id,
                        temperature = currentTemperature,
                        humidity = currentHumidity
                    };
                    var messageString = JsonConvert.SerializeObject(telemetryDataPoint);
                    var message = new Microsoft.Azure.Devices.Client.Message(Encoding.ASCII.GetBytes(messageString));
                    message.Properties.Add("messageId", _messageId.ToString());
                    message.Properties.Add("temperatureAlert", (currentTemperature > 30) ? "true" : "false");

                    DeviceClient deviceClient = DeviceClient.Create(
                        iotHubUri,
                        new DeviceAuthenticationWithRegistrySymmetricKey(device.Id, device.Authentication.SymmetricKey.PrimaryKey));

                    tasks.Add(deviceClient.SendEventAsync(message));

                    Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);
                }
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}
