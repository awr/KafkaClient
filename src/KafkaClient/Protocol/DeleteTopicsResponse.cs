using System;
using System.Collections.Generic;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Response (Version: 0) => [topic_error_codes] 
    ///  topic_error_codes => topic error_code 
    ///    topic => STRING
    ///    error_code => INT16
    /// </summary>
    public class DeleteTopicsResponse : TopicsResponse
    {
        public static DeleteTopicsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topics = new Topic[reader.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    topics[i] = new Topic(topicName, errorCode);
                }
                return new DeleteTopicsResponse(topics);
            }
        }

        public DeleteTopicsResponse(IEnumerable<Topic> topics = null)
            : base (topics)
        {
        }
    }
}