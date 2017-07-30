using System;
using System.Collections.Generic;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Response => [topic_error_codes] 
    ///  topic_error_codes => topic error_code 
    ///    topic => STRING
    ///    error_code => INT16
    /// </summary>
    public class CreateTopicsResponse : TopicsResponse
    {
        public static CreateTopicsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topics = new Topic[reader.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    topics[i] = new Topic(topicName, errorCode);
                }
                return new CreateTopicsResponse(topics);
            }
        }

        public CreateTopicsResponse(IEnumerable<Topic> topics = null)
            : base (topics)
        {
        }
    }
}