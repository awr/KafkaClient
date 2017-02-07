using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient;

namespace KafkaClient.Examples
{
    public class SimpleConsumer : IExample
    {
        public async Task RunAsync(KafkaOptions options, string topic)
        {
            using (var source = new CancellationTokenSource()) {
                // trigger cancellation when the user hits the ENTER key in the console
                Console.WriteLine("Hit the ENTER key to stop the receiver ...");
                var endTask = Task.Run(
                    () => {
                        Console.ReadLine();
                        source.Cancel();
                    });

                using (var consumer = await options.CreateConsumerAsync()) {
                    var fetchTask = consumer.FetchAsync(
                        message => Console.WriteLine($"{topic}: {message.Value.ToString()}"),
                        topic,
                        0, // partition id
                        source.Token);
                    await Task.WhenAny(fetchTask, endTask);
                }
            }
        }
    }
}