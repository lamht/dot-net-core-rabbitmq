using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.IO;
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
                string filePath = AppDomain.CurrentDomain.BaseDirectory + "final_ean.csv";
                CsvParserOptions csvParserOptions = new CsvParserOptions(true, ',');
                CsvSmsMapping csvMapper = new CsvSmsMapping();
                CsvParser<EanStartDate> csvParser = new CsvParser<EanStartDate>(csvParserOptions, csvMapper);
                var result = csvParser.ReadFromFile(filePath, Encoding.ASCII).ToList();
                foreach (var details in result)
                {
                    string ean = details.Result.Ean;
                    DateTime startDate = PraseDate(details.Result.StartDate);
                    DateTime endDate = new DateTime(2021, 10, 1);
                    WriteFile(ean);
                    PushEvent(startDate, endDate, ean, channel);
                }
            }
        }

        private static DateTime PraseDate(string date)
        {
            String[] dateAndTime = date.Split(" ");
            String dateOnly = dateAndTime[0];
            String[] dayMonthYear = dateOnly.Split("/");
            int year = int.Parse(dayMonthYear[2]);
            int month = int.Parse(dayMonthYear[1]);
            int day = int.Parse(dayMonthYear[0]);
            return new DateTime(year, month, day);

        }
        private static void PushEvent(DateTime startDate, DateTime endDate, String ean , IModel channel)
        {
            DateTime currentDate = startDate;
            while(currentDate <= endDate)
            {
                P4UsageToCoreEvent eanEvent = new P4UsageToCoreEvent
                {
                    EanId = ean,
                    QueryDate = currentDate,
                    QueryReason = "DAY"
                };
                string message = JsonConvert.SerializeObject(eanEvent);
                Console.WriteLine($"push {message}");
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(
                    exchange: "kikker_event_bus",
                    routingKey: "P4UsageToCoreEvent",
                    basicProperties: null,
                    body: body
                );
                currentDate = currentDate.AddDays(1);
                Thread.Sleep(400);
            }
        }

        private static void WriteFile(String message)
        {
            using (StreamWriter writer = System.IO.File.AppendText("logfile.txt"))
            {
                writer.WriteLine(message);
            }
        }

        private class CsvSmsMapping : CsvMapping<EanStartDate>
        {
            public CsvSmsMapping() : base()
            {
                MapProperty(0, x => x.Ean);
                MapProperty(1, x => x.StartDate);
            }
        }

        public class EanStartDate
        {
            public string Ean { get; set; }
            public string StartDate { get; set; }
        }

        public class P4UsageToCoreEvent
        {
            public string EanId { get; set; }
            public DateTime QueryDate { get; set; }
            public string QueryReason { get; set; }
        }
    }
}
