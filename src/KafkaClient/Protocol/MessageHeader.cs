using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Header => HeaderKey HeaderVal
    /// </summary>
    /// <remarks>
    /// Header => HeaderKey HeaderVal
    ///   HeaderKeyLen => varint
    ///   HeaderKey => string
    ///   HeaderValueLen => varint
    ///   HeaderValue => data
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </remarks>
    public class MessageHeader : IEquatable<MessageHeader>
    {
        public MessageHeader(string key, ArraySegment<byte> value)
        {
            Key = key;
            Value = value;
        }

        /// <summary>
        /// Key value for the header
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// Value data for the header
        /// </summary>
        public ArraySegment<byte> Value { get; }

        public bool Equals(MessageHeader other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Key, other.Key) 
                && Value.Equals(other.Value);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as MessageHeader);
        }

        public override int GetHashCode()
        {
            unchecked {
                return ((Key != null ? Key.GetHashCode() : 0) * 397) ^ Value.GetHashCode();
            }
        }
    }
}