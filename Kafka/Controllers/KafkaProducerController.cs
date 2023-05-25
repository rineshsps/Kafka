using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Controllers
{
    [Route("api/kafka")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig
        { BootstrapServers = "localhost:9092" };
        private readonly string topic = "simpletalk_topic";
        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {

           var result =  SendToKafka(topic, message);

            // To get total count
            GetMessageCount(topic);

            return Created(string.Empty, result);

        }
        private Object SendToKafka(string topic, string message)
        {
            using (var producer =
                 new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Oops, something went wrong: {e}");
                }
            }

           
            return null;
        }

        private long GetMessageCount(string topic)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", // Replace with your Kafka broker's address
                GroupId = "my-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                //TopicPartition topicPartition = new TopicPartition(topic, Partition.Any);
                TopicPartition topicPartition = new TopicPartition(topic, new Partition(0));

                var watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(5));

                if (watermarkOffsets.High >= watermarkOffsets.Low)
                {
                    long messageCount = watermarkOffsets.High - watermarkOffsets.Low;
                    Console.WriteLine($"Total message count in topic '{topic}': {messageCount}");
                    return messageCount;
                }
            }

            return 0;
        }

    }
}