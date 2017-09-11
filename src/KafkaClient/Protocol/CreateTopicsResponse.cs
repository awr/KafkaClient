using System;
using System.Collections.Generic;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Response => *throttle_time_ms [topic_errors] 
    /// </summary>
    /// <remarks>
    /// CreateTopics Response => *throttle_time_ms [topic_errors] 
    ///   throttle_time_ms => INT32
    ///   topic_errors => topic error_code *error_message 
    ///     topic => STRING
    ///     error_code => INT16
    ///     error_message => NULLABLE_STRING
    ///
    /// Version 1+: error_message
    /// Version 2+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
    /// </remarks>
    public class CreateTopicsResponse : TopicsResponse, IEquatable<CreateTopicsResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},topic_errors:{Topics.ToStrings()}}}";

        public static CreateTopicsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 2);
                var topicCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(topicCount);
                var topics = new Topic[topicCount];
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    string errorMessage = null;
                    if (context.ApiVersion >= 1) {
                        errorMessage = reader.ReadString();
                    }
                    topics[i] = new Topic(topicName, errorCode, errorMessage);
                }
                return new CreateTopicsResponse(topics, throttleTime);
            }
        }

        public CreateTopicsResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base (topics, throttleTime)
        {
        }

        public bool Equals(CreateTopicsResponse other)
        {
            return Equals(other as TopicsResponse);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as CreateTopicsResponse);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}