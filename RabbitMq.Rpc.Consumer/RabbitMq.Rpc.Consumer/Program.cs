using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System.Text;

namespace RabbitMq.Rpc.Consumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            ConnectionFactory factory = new ConnectionFactory();
            factory.Port = 5672;
            factory.HostName = "localhost";
            factory.UserName = "guest";
            factory.Password = "guest";
            factory.VirtualHost = "/";

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "rpc_queue",
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: "rpc_queue",
                                 autoAck: false,
                                 consumer: consumer);
            Console.WriteLine(" [x] Awaiting RPC requests");

            consumer.Received += (model, ea) =>
            {
                string response = string.Empty;

                var body = ea.Body.ToArray();
                var props = ea.BasicProperties;
                var replyProps = channel.CreateBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                try
                {
                    var message = Encoding.UTF8.GetString(body);
                    var messagesplit = message.Split(',');
                    string firstnum = messagesplit[0];
                    string secondnum = messagesplit[1];
                    string operation = messagesplit[2];

                    int n1 = int.Parse(firstnum);
                    int n2 = int.Parse(secondnum);

                    Console.WriteLine($" Received [Number 1] {firstnum}");
                    Console.WriteLine($" Received [Number 2] {secondnum}");
                    Console.WriteLine($" Received [operation] {operation}");
                    Console.WriteLine($" CorrelationId {props.CorrelationId}");

                    response = Convert.ToString(Process(n1, n2, operation));

                    Console.WriteLine($" Sending Response [Total :- ] {response}");
                    Console.WriteLine("-----------------------------------");
                }
                catch (Exception e)
                {
                    Console.WriteLine($" [.] {e.Message}");
                    response = string.Empty;
                }
                finally
                {
                    var responseBytes = Encoding.UTF8.GetBytes(response);
                    channel.BasicPublish(exchange: string.Empty,
                                         routingKey: props.ReplyTo,
                                         basicProperties: replyProps,
                                         body: responseBytes);
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
            };

            Console.WriteLine(" Press [enter] to exit.");

            Console.ReadLine();
        }

        static int Process(int n1, int n2, string operation)
        {
            int n3 = 0;
            if (operation == "+")
            {
                n3 = n1 + n2;
            }

            return n3;
        }
    }
}