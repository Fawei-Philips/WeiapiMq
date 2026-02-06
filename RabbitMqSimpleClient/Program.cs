// See https://aka.ms/new-console-template for more information

using RabbitMqSimpleClient.Clients;

await new QueueModeConsumer().ConsumeAsync();


Console.ReadLine();
