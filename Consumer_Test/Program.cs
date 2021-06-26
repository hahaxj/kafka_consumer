using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using io.confluent.ksql.avro_schemas;
using Newtonsoft.Json;
using System;
using System.Linq;

namespace Consumer_Test
{
    class Program
    {
        private static readonly string topicName = "NONPHARM_CPOY_LIVE";// "NONPHARM_LIVE";// "ca6340e403ac.dbo.Items"; //DEMO_LIVE_1   
        private static readonly string ipinfo = "172.27.0.189";
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var config = new ConsumerConfig
            {
                GroupId = "consumers_test_22",
                BootstrapServers = $"{ipinfo}:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.Plaintext, 
            };

            var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                SchemaRegistryUrl = $"http://{ipinfo}:8081"
            });
            //SchemaAvroSerDes


            //using (var consumer = new ConsumerBuilder<Ignore, string>(config)
            //    // Note: All handlers are called on the main .Consume thread.
            //    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            //    .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
            //    .SetPartitionsAssignedHandler((c, partitions) =>
            //    {
            //        // Since a cooperative assignor (CooperativeSticky) has been configured, the
            //        // partition assignment is incremental (adds partitions to any existing assignment).
            //        Console.WriteLine(
            //            "Partitions incrementally assigned: [" +
            //            string.Join(',', partitions.Select(p => p.Partition.Value)) +
            //            "], all: [" +
            //            string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
            //            "]");

            //        // Possibly manually specify start offsets by returning a list of topic/partition/offsets
            //        // to assign to, e.g.:
            //        // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
            //    })
            //    .SetPartitionsRevokedHandler((c, partitions) =>
            //    {
            //        // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
            //        // assignment is incremental (may remove only some partitions of the current assignment).
            //        var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
            //        Console.WriteLine(
            //            "Partitions incrementally revoked: [" +
            //            string.Join(',', partitions.Select(p => p.Partition.Value)) +
            //            "], remaining: [" +
            //            string.Join(',', remaining.Select(p => p.Partition.Value)) +
            //            "]");
            //    })
            //    .Build())
            //{
            //    consumer.Subscribe(topicName);

            //    try
            //    {
            //        while (true)
            //        {
            //            try
            //            {
            //                var consumeResult = consumer.Consume();

            //                if (consumeResult.IsPartitionEOF)
            //                {
            //                    Console.WriteLine(
            //                        $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

            //                    continue;
            //                }

            //                Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

            //                try
            //                {
            //                    consumer.Commit(consumeResult);
            //                }
            //                catch (KafkaException e)
            //                {
            //                    Console.WriteLine($"Commit error: {e.Error.Reason}");
            //                }
            //            }
            //            catch (ConsumeException e)
            //            {
            //                Console.WriteLine($"Consume error: {e.Error.Reason}");
            //            }
            //        }
            //    }
            //    catch (OperationCanceledException)
            //    {
            //        Console.WriteLine("Closing consumer.");
            //        consumer.Close();
            //    }
            //}

            //.SetValueDeserializer(new AvroDeserializer<GenericRecord>())
            using (var consumer = new ConsumerBuilder<int, GenericRecord>(config).SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync()).Build())
            //using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {

                consumer.Subscribe(topicName);
                //var partion = new TopicPartition(topicName, new Partition(0));
                //consumer.Assign(partion);
                //consumer.Seek(new TopicPartitionOffset(partion, 0));
                int total = 0;

                while (true)
                {
                    
                    var cr = consumer.Consume();



                    //try to get real model
                    var valueString = cr.Message.Value.ToString().Split("contents:")[1];

                    var model = JsonConvert.DeserializeObject<TestModel>(valueString);// JsonSerializer.d

                    Console.WriteLine(cr.Message.Key + "----" + model.ToString());
                }
            }
                Console.Read();


        }
    }
}
