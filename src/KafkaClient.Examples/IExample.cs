using System.Threading.Tasks;

namespace KafkaClient.Examples
{
    public interface IExample
    {
        Task RunAsync(KafkaOptions options, string topic);
    }
}