using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client;
using System.Text;

namespace RabbitMQBenchmark.Publisher
{
    public class Program
    {
        const string exchange = "massive.ingestion";
        public static void Main(string[] args)
        {
            //args = new string[] { "5672", "subtitles.german", "1000000" };   //5672 5674 5676   //"subtitles.*","metadata.*"

            string port = args[0];
            string topic = args[1];
            int limit = int.Parse(args[2]);

            var factory = new ConnectionFactory() { HostName = "localhost", Port = int.Parse(port) };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange, ExchangeType.Topic, durable: true);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;
                properties.DeliveryMode = 2;
                                
                

                for (int i = 1; i <= limit; i++)
                {
                    string message = $"[Message {i}]";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: exchange,
                                 routingKey: topic,
                                 basicProperties: properties,
                                 body: body);

                    Console.WriteLine($"Sent {message} on topic [{topic}]");
                }


                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }

        }
    }
}
