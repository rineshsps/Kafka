using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;
namespace Kafka.Handlers
{
    public class KafkaConsumerHandler : IHostedService
    {
        private readonly string topic = "simpletalk_topic";
        public Task StartAsync(CancellationToken cancellationToken)
        {
            var conf = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", // Replace with your Kafka broker's address
                GroupId = "my-consumer-group",
                //AutoOffsetReset = AutoOffsetReset.Earliest,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };


            //read from Beginning
            //using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            //{
            //    TopicPartitionOffset topicPartitionOffset = new TopicPartitionOffset(topic, 0, Offset.Beginning);

            //    consumer.Assign(new List<TopicPartitionOffset> { topicPartitionOffset });

            //    consumer.Seek(topicPartitionOffset);

            //    while (true)
            //    {
            //        var consumeResult = consumer.Consume();

            //        // Process the consumed message
            //        Console.WriteLine($"Received message: {consumeResult.Message.Value}");
            //    }
            //}

            using (var builder = new ConsumerBuilder<Ignore,
                string>(conf).Build())
            {
                builder.Subscribe(topic);
                var cancelToken = new CancellationTokenSource();
                try
                {
                    while (true)
                    {
                        var consumer = builder.Consume(cancelToken.Token);
                        Console.WriteLine($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
                    }
                }
                catch (Exception)
                {
                    builder.Close();
                }
            }
            return Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
