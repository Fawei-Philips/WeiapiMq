using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace RabbitMqSimpleClient.Clients
{
    public class TopicModeConsumer
    {
        private readonly ConnectionFactory connectionFactory;
        private const string RabbitMqExchangeName = "doraemon.exchange";
        private const string TopicPattern = "doraemon.*";
        private const string QueueName = "doraemon.queue";
        private IConnection connection;
        private IChannel channel;
        private AsyncEventingBasicConsumer consumer;

        public TopicModeConsumer()
        {
            connectionFactory = new ConnectionFactory
            {
                HostName = MqConstants.HostName,
                Port = MqConstants.Port,
                UserName = MqConstants.UserName,
                Password = MqConstants.Password,
                VirtualHost = MqConstants.VirtualHost
            };
        }

        public async Task ConsumeAsync()
        {
            connection = await connectionFactory.CreateConnectionAsync();
            channel = await connection.CreateChannelAsync();

            await channel.ExchangeDeclareAsync(RabbitMqExchangeName, ExchangeType.Topic, true);
            await channel.BasicQosAsync(0, 1, false);

            await channel.QueueBindAsync(queue: QueueName, exchange: RabbitMqExchangeName, routingKey: TopicPattern);

            consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += ReceiveQueuedMessageAsync;

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer);

        }

        private async Task ReceiveQueuedMessageAsync(object sender, BasicDeliverEventArgs @event)
        {
            var body = @event.Body.Span;
            var message = Encoding.UTF8.GetString(body);

            Debug.WriteLine(message);

            await channel.BasicAckAsync(@event.DeliveryTag, false);
            await Task.Delay(1000);
        }
    }
}
