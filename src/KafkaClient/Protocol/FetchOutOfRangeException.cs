using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for fetching (FetchRequest, FetchOffset, etc) having bad offsets
    /// </summary>
    public class FetchOutOfRangeException : RequestException
    {
        public FetchOutOfRangeException(FetchRequest.Topic topic, ErrorCode errorCode, Endpoint endpoint)
            : base(ApiKey.Fetch, errorCode, endpoint, topic.ToString())
        {
            _topic = topic;
        }

        // ReSharper disable once NotAccessedField.Local -- for debugging
        private readonly FetchRequest.Topic _topic;
    }
}