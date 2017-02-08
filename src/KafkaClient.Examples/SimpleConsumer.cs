using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Examples
{
    public class SimpleConsumer : IExample
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

                using (var consumer = await options.CreateConsumerAsync(topic, 0)) {
                    var consumeTask = consumer.ConsumeAsync(
                        message => Console.WriteLine($"{topic}: {message.Value.ToString()}"),
                        source.Token);
                    await Task.WhenAny(consumeTask, endTask);
                }
            }
        }
    }
}