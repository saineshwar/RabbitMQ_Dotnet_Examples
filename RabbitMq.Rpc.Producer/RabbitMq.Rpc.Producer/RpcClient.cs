using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RabbitMq.Rpc.Producer
{
    public class RpcClient
    {
        private const string QUEUE_NAME = "rpc_queue";

        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> callbackMapper = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

        public Task<string> CallAsync(string message, IConnection connection, IModel channel,string correlationId, CancellationToken cancellationToken = default)
        {
            string replyQueueName;

            try
            {
                // declare a server-named queue
                replyQueueName = channel.QueueDeclare().QueueName;
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    if (!callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out var tcs))
                        return;
                    var body = ea.Body.ToArray();
                    var response = Encoding.UTF8.GetString(body);
                    tcs.TrySetResult(response);
                };

                channel.BasicConsume(consumer: consumer, queue: replyQueueName, autoAck: true);


                IBasicProperties props = channel.CreateBasicProperties();
               
                props.CorrelationId = correlationId;
                props.ReplyTo = replyQueueName;

                var messageBytes = Encoding.UTF8.GetBytes(message);
                var tcs = new TaskCompletionSource<string>();
                callbackMapper.TryAdd(correlationId, tcs);

                channel.BasicPublish(exchange: string.Empty,
                                     routingKey: QUEUE_NAME,
                                     basicProperties: props,
                                     body: messageBytes);

                cancellationToken.Register(() => callbackMapper.TryRemove(correlationId, out _));
                return tcs.Task;
            }
            catch (Exception)
            {

                throw;
            }

        }


    }
}
