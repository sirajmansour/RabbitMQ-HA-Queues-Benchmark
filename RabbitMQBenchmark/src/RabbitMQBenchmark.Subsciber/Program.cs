using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQBenchmark.Subscriber
{
    public class Program
    {
        const string exchange = "massive.ingestion";
        public static void Main(string[] args)
        {
            //args = new string[] { "5672", "subtitles.*" };   //5672 5674 5676   //"subtitles.*","metadata.*"

            string port = args[0]; 
            string bindingKey = args[1]; 

            string queueName = bindingKey.Split(new char[] { '.' })[0];

            var factory = new ConnectionFactory() { HostName = "localhost" , Port = int.Parse(port) };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange, "topic", durable: true, autoDelete:false);

                channel.QueueDeclare(queueName, durable : true , autoDelete : false, exclusive:false);

                channel.QueueBind(queue: queueName,
                                  exchange: exchange,
                                  routingKey: bindingKey);

                var consumer = new EventingBasicConsumer(channel);
                int i = 1;

                consumer.Received += (model, e) =>
                {
                    var body = e.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = e.RoutingKey;
                    Console.WriteLine("Received [{0}] on [{1}]", message, routingKey);

                    channel.BasicAck(e.DeliveryTag, multiple: false);
                };

                
                channel.BasicConsume(queue: queueName,
                                     noAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
