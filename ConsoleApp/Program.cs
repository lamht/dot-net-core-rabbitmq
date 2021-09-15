using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(
                    queue: "Core",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );
                string filePath = AppDomain.CurrentDomain.BaseDirectory + "phone_number_csv.csv";
                CsvParserOptions csvParserOptions = new CsvParserOptions(true, ',');
                CsvSmsMapping csvMapper = new CsvSmsMapping();
                CsvParser<SendSmsEvent> csvParser = new CsvParser<SendSmsEvent>(csvParserOptions, csvMapper);
                var result = csvParser.ReadFromFile(filePath, Encoding.ASCII).ToList();
                foreach (var details in result)
                {
                    Console.WriteLine(details.Result.PhoneNumberTo + " " + details.Result.Message);
                    SendSmsEvent sendSmsEvent = new SendSmsEvent
                    {
                        PhoneNumberTo = details.Result.PhoneNumberTo,
                        Message = details.Result.Message
                    };
                    string message = JsonConvert.SerializeObject(sendSmsEvent);
                    var body = Encoding.UTF8.GetBytes(message);
                    channel.BasicPublish(
                        exchange: "kikker_event_bus",
                        routingKey: "SendSmsEvent",
                        basicProperties: null,
                        body: body
                    );
                    Thread.Sleep(1000);
                }
            }
        }

        private class CsvSmsMapping : CsvMapping<SendSmsEvent>
        {
            public CsvSmsMapping() : base()
            {
                MapProperty(0, x => x.PhoneNumberTo);
                MapProperty(1, x => x.Message);
            }
        }

        public class SendSmsEvent
        {
            public string PhoneNumberTo { get; set; }
            public string Message { get; set; }
        }
    }
}
