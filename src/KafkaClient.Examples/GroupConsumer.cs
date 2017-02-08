using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;

namespace KafkaClient.Examples
{
    public class GroupConsumer : IExample
    {
        public async Task RunAsync(KafkaOptions options, string topic)
        {
            using (var source = new CancellationTokenSource()) {
                // trigger cancellation when the user hits the ENTER key in the console
                Console.WriteLine("Hit the ENTER key to stop the consumer ...");
                var endTask = Task.Run(
                    () => {
                        Console.ReadLine();
                        source.Cancel();
                    });

                var groupId = Guid.NewGuid().ToString("N");
                using (var router = await options.CreateRouterAsync()) {
                    var tasks = new List<Task>();
                    for (var index = 0; index < 2; index++) {
                        var consumer = await options.CreateGroupConsumerAsync(router, groupId, new ConsumerProtocolMetadata(topic), source.Token);
                        tasks.Add(consumer.ConsumeAsync(
                            message => Console.WriteLine($"{topic}: {message.Value.ToString()}"),
                            source.Token));
                    }
                    var consumeTasks = Task.WhenAll(tasks);
                    await Task.WhenAny(consumeTasks, endTask);
                }
            }            
        }
    }
}