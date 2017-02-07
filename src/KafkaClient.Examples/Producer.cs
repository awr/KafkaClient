using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient.Examples
{
    public class Producer : IExample
    {
        public async Task RunAsync(KafkaOptions options, string topic)
        {
            // create some messages for sending
            var messages = Enumerable.Range(0, 100).Select(i => new Message($"Value {i}", i.ToString()));

            using (var producer = await options.CreateProducerAsync()) {
                await producer.SendAsync(messages, topic, CancellationToken.None);
            }
        }
    }
}