using RabbitMQ.Client;
using System.Net.Sockets;

namespace RabbitMq.Rpc.Producer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.Port = 5672;
            factory.HostName = "localhost";
            factory.UserName = "guest";
            factory.Password = "guest";
            factory.VirtualHost = "/";


            IConnection connection;
            IModel channel;
            connection = factory.CreateConnection();
            channel = connection.CreateModel();

            try
            {
                while (true)
                {
                    for (int j = 0; j < 5; j++)
                    {

                        for (int i = 0; i < 50; i++)
                        {
                            Random rnd = new Random();
                            int number1 = rnd.Next(1, 8888);
                            int number2 = rnd.Next(1, 9999);

                            int firstnum = number1;
                            int secondnum = number2;

                            await InvokeAsync(firstnum, secondnum, "+", connection, channel);
                        }
                        Thread.Sleep(1000);
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }

        }

        private static async Task InvokeAsync(int n1, int n2, string operation, IConnection connection, IModel channel)
        {
            var rpcClient = new RpcClient();
            string message = string.Join(",", Convert.ToString(n1), Convert.ToString(n2), operation);
            var correlationId = Guid.NewGuid().ToString();

            Console.WriteLine("Sent Number_1 :- {0}", Convert.ToString(n1));
            Console.WriteLine("Sent Number_2 :- {0}", Convert.ToString(n2));
            Console.WriteLine("Sent Operation :- {0}", Convert.ToString(operation));
            Console.WriteLine("CorrelationId :- {0}", Convert.ToString(correlationId));
          

            var response = await rpcClient.CallAsync(message, connection, channel, correlationId);
            Console.WriteLine(" [Received Response]  '{0}'", response);
            Console.WriteLine("-----------------------------------");
        }
    }
}