using System;
using System.Collections.Generic;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Response => *throttle_time_ms [topic_error_codes] 
    /// </summary>
    /// <remarks>
    /// DeleteTopics Response => *throttle_time_ms [topic_error_codes] 
    ///   throttle_time_ms => INT32
    ///   topic_error_codes => topic error_code 
    ///     topic => STRING
    ///     error_code => INT16
    /// 
    /// Version 1+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
    /// </remarks>
    public class DeleteTopicsResponse : TopicsResponse, IEquatable<DeleteTopicsResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},topic_errors:{Topics.ToStrings()}}}";

        public static DeleteTopicsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                var topics = new Topic[reader.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    topics[i] = new Topic(topicName, errorCode);
                }
                return new DeleteTopicsResponse(topics, throttleTime);
            }
        }

        public DeleteTopicsResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base (topics, throttleTime)
        {
        }

        public bool Equals(DeleteTopicsResponse other)
        {
            return Equals((TopicsResponse) other);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteTopicsResponse);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}