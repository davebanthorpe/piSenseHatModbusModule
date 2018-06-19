namespace piSenseHatModbusModule
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json;
    using System.Collections.Generic;     // for KeyValuePair<>
    using Microsoft.Azure.Devices.Shared;

    class Program
    {
        static int counter;
        
        // Twin module variables
        static int redThreshold { get; set; } = 27;
        static int amberThreshold { get; set; } = 25;
        static int greenThreshold { get; set; } = 23;
        static int cyanThreshold { get; set; } = 21;
        static int blueThreshold {get;set; } = 19;
        static int tempCorr { get; set; } = 6;

        static void Main(string[] args)
        {
            // The Edge runtime gives us the connection string we need -- it is injected as an environment variable
            string connectionString = Environment.GetEnvironmentVariable("EdgeHubConnectionString");

            // Cert verification is not yet fully functional when using Windows OS for the container
            bool bypassCertVerification = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            if (!bypassCertVerification) InstallCert();
            Init(connectionString, bypassCertVerification).Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Add certificate in local cert store for use by client for secure connection to IoT Edge runtime
        /// </summary>
        static void InstallCert()
        {
            string certPath = Environment.GetEnvironmentVariable("EdgeModuleCACertificateFile");
            if (string.IsNullOrWhiteSpace(certPath))
            {
                // We cannot proceed further without a proper cert file
                Console.WriteLine($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing path to certificate file.");
            }
            else if (!File.Exists(certPath))
            {
                // We cannot proceed further without a proper cert file
                Console.WriteLine($"Missing path to certificate collection file: {certPath}");
                throw new InvalidOperationException("Missing certificate file.");
            }
            X509Store store = new X509Store(StoreName.Root, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            store.Add(new X509Certificate2(X509Certificate2.CreateFromCertFile(certPath)));
            Console.WriteLine("Added Cert: " + certPath);
            store.Close();
        }


        /// <summary>
        /// Initializes the DeviceClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init(string connectionString, bool bypassCertVerification = false)
        {
            Console.WriteLine("Connection String {0}", connectionString);

            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            // During dev you might want to bypass the cert verification. It is highly recommended to verify certs systematically in production
            if (bypassCertVerification)
            {
                mqttSetting.RemoteCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
            }
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            DeviceClient ioTHubModuleClient = DeviceClient.CreateFromConnectionString(connectionString, settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            // await ioTHubModuleClient.SetImputMessageHandlerAsync("input1", PipeMessage, iotHubModuleClient);

            // Read redThreshold from Module Twin Desired Properties
            var moduleTwin = await ioTHubModuleClient.GetTwinAsync();
            var moduleTwinCollection = moduleTwin.Properties.Desired;
            try {
                redThreshold = moduleTwinCollection["redThreshold"];
            } catch(ArgumentOutOfRangeException e) {
                Console.WriteLine("Property redThreshold not exist");
            }
            try {
                amberThreshold = moduleTwinCollection["amberThreshold"];
            } catch(ArgumentOutOfRangeException e) {
                Console.WriteLine("Property amberThreshold not exist");
            }
            try {
                greenThreshold = moduleTwinCollection["greenThreshold"];
            } catch(ArgumentOutOfRangeException e) {
                Console.WriteLine("Property greenThreshold not exist");
            }
            try {
                cyanThreshold = moduleTwinCollection["cyanThreshold"];
            } catch(ArgumentOutOfRangeException e) {
                Console.WriteLine("Property cyanThreshold not exist");
            }
            try {
                blueThreshold = moduleTwinCollection["blueThreshold"];
            } catch(ArgumentOutOfRangeException e) {
                Console.WriteLine("Property blueThreshold not exist");
            }
            try
            {
                tempCorr = moduleTwinCollection["tempCorr"];
            }
            catch (ArgumentOutOfRangeException e)
            {
                Console.WriteLine("Property tempCorr not exist");
            }



            // Attach callback for Twin desired properties updates
            await ioTHubModuleClient.SetDesiredPropertyUpdateCallbackAsync(onDesiredPropertiesUpdate, null);

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", CheckTemperature, ioTHubModuleClient);
        }

        static async Task<MessageResponse> CheckTemperature(Message message, object userContext)
        {
            var counterValue = Interlocked.Increment(ref counter);


            try {
                DeviceClient deviceClient = (DeviceClient)userContext;

                var messageBytes = message.GetBytes();
                var messageString = Encoding.UTF8.GetString(messageBytes);
                Console.WriteLine($"Received message {counterValue}: [{messageString}]");

                if (!string.IsNullOrEmpty(messageString))
                {
                    // Get the body of the message and deserialize it
                    var dataBody = JsonConvert.DeserializeObject<Data[]>(messageString);
                    Console.WriteLine("Done. Count = " + dataBody.Length);

                    if (dataBody.Length >= 1)
                    {
                        for(int i = 0; i < dataBody.Length; i++)
                        {
                            var hwID = dataBody[i].HwId;
                            if (dataBody[i].DisplayName.Equals("Temperature"))
                            {
                                float currentTemp = Int32.Parse(dataBody[i].Value);
                                currentTemp = currentTemp / 10;
                                Console.WriteLine($"RED: {redThreshold}  AMBER: {amberThreshold}  GREEN: {greenThreshold}");
                                if (currentTemp > redThreshold) {
                                    Console.WriteLine("Temp RED alarm: " + currentTemp);
                                    using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"2\"}"))
                                    {
                                        var deviceMessage = new Message(stream);
                                        deviceMessage.Properties.Add("command-type","ModbusWrite");
                                        await deviceClient.SendEventAsync("output1", deviceMessage);
                                    }  
                                } else if (currentTemp > amberThreshold) {
                                    Console.WriteLine("Temp AMBER alarm: " + currentTemp);
                                    using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"1\"}"))
                                    {
                                        var deviceMessage = new Message(stream);
                                        deviceMessage.Properties.Add("command-type","ModbusWrite");
                                        await deviceClient.SendEventAsync("output1", deviceMessage);
                                    }                          
                                } else if (currentTemp < greenThreshold) {
                                    Console.WriteLine("Temp GREEN no alarm: " + currentTemp);
                                    using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40011\",\"Value\":\"0\"}"))
                                    {
                                        var deviceMessage = new Message(stream);
                                        deviceMessage.Properties.Add("command-type","ModbusWrite");
                                        await deviceClient.SendEventAsync("output1", deviceMessage);
                                    }                          
                                }
                                using (var stream = GenerateStreamFromString("{\"HwId\":\"" + hwID + "\",\"UId\":\"1\",\"Address\":\"40012\",\"Value\":\"" + tempCorr + "\"}"))
                                {
                                    var deviceMessage = new Message(stream);
                                    deviceMessage.Properties.Add("command-type", "ModbusWrite");
                                    await deviceClient.SendEventAsync("output1", deviceMessage);
                                }

                            }  
                    
                        }

                    } else {
                        // do nothing;
                    }
                    
                }

                // Indicate that the message treatment is completed
                return MessageResponse.Completed;
            }
            catch (AggregateException ex)
            {
                foreach (Exception exception in ex.InnerExceptions)
                {
                    Console.WriteLine();
                    Console.WriteLine("Error in sample: {0}", exception);
                }
                // Indicate that the message treatment is not completed
                var deviceClient = (DeviceClient)userContext;
                return MessageResponse.Abandoned;
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in sample: {0}", ex.Message);
                // Indicate that the message treatment is not completed
                DeviceClient deviceClient = (DeviceClient)userContext;
                return MessageResponse.Abandoned;
            }
        }   


        static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }


        static Task onDesiredPropertiesUpdate(TwinCollection desiredProperties, object userContext)
        {
            try
            {
                Console.WriteLine("Desired property change:");
                Console.WriteLine(JsonConvert.SerializeObject(desiredProperties));

                if (desiredProperties["redThreshold"]!=null)
                    redThreshold = desiredProperties["redThreshold"];
                if (desiredProperties["amberThreshold"]!=null)
                    amberThreshold = desiredProperties["amberThreshold"];
                if (desiredProperties["greenThreshold"]!=null)
                    greenThreshold = desiredProperties["greenThreshold"];
                if (desiredProperties["cyanThreshold"]!=null)
                    cyanThreshold = desiredProperties["cyanThreshold"];
                if (desiredProperties["blueThreshold"]!=null)
                    blueThreshold = desiredProperties["blueThreshold"];
                if (desiredProperties["tempCorr"] != null) { 
                    tempCorr = desiredProperties["tempCorr"];
                }

            }
            catch (AggregateException ex)
            {
                foreach (Exception exception in ex.InnerExceptions)
                {
                    Console.WriteLine();
                    Console.WriteLine("Error when receiving desired property: {0}", exception);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error when receiving desired property: {0}", ex.Message);
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var deviceClient = userContext as DeviceClient;
            if (deviceClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                var pipeMessage = new Message(messageBytes);
                foreach (var prop in message.Properties)
                {
                    pipeMessage.Properties.Add(prop.Key, prop.Value);
                }
                await deviceClient.SendEventAsync("output1", pipeMessage);
                Console.WriteLine("Received message sent");
            }
            return MessageResponse.Completed;
        }

        public class Data
        {
            public string DisplayName { get; set; }
            public string HwId { get; set; }
            public string Address { get; set; }
            public string Value { get; set; }
            public string SourceTimestamp { get; set; }
        }

        public class DeviceMessage
        {
            public string HwId { get; set;}
            public string UId { get; set;}
            public string Address {get ; set;}
            public string Value {get ; set;}
        }
    }
}
