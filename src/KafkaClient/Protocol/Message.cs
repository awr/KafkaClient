using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Record => Length Attributes TimestampDelta OffsetDelta Key Value [Header] 
    /// </summary>
    /// <remarks>
    /// Version 2+:
    /// Record => Length Attributes TimestampDelta OffsetDelta Key Value [Header] 
    ///   Length => varint
    ///   Attributes => int8
    ///   TimestampDelta => varint
    ///   OffsetDelta => varint
    ///   KeyLen => varint
    ///   Key => data
    ///   ValueLen => varint
    ///   Value => data
    ///   Headers => [Header]
    /// 
    /// Version 0-1:
    /// Message represents the data from a single event occurance.
    /// Message => Crc MagicByte Attributes *Timestamp Key Value
    ///   Crc => int32
    ///   MagicByte => int8
    ///   Attributes => int8
    ///    bit 0 ~ 2 : Compression codec.
    ///      0 : no compression
    ///      1 : gzip
    ///      2 : snappy
    ///      3 : lz4
    ///    bit 3 : Timestamp type
    ///      0 : create time
    ///      1 : log append time
    ///    bit 4 ~ 7 : reserved
    ///   Timestamp => int64
    ///   Key => bytes
    ///   Value => bytes
    /// 
    /// Version 1 only: Timestamp
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </remarks>>
    public class Message : IEquatable<Message>
    {
        public override string ToString() => $"{{KeySize:{Key.Count},ValueSize:{Value.Count},Offset:{Offset}}}";

        public int WriteTo(IKafkaWriter writer, byte version, MessageCodec codec = MessageCodec.None, long firstOffset = 0L, DateTimeOffset? firstTimestamp = null)
        {
            // Record was introduced with version 2, and is significantly different from the Message approach (see above)
            return version >= 2 
                ? WriteRecord(writer, codec, firstOffset, firstTimestamp) 
                : WriteMessage(writer, version, codec);
        }

        private int WriteMessage(IKafkaWriter writer, byte version, MessageCodec codec = MessageCodec.None)
        {
            using (writer.MarkForCrc()) {
                writer.Write(version)
                      .Write((byte) codec);
                if (version >= 1) {
                    writer.Write(Timestamp.GetValueOrDefault(DateTimeOffset.UtcNow).ToUnixTimeMilliseconds());
                }
                writer.Write(Key);

                if (codec == MessageCodec.None) {
                    writer.Write(Value);
                    return 0;
                }
                using (writer.MarkForLength()) {
                    var initialPosition = writer.Position;
                    writer.WriteCompressed(Value, codec);
                    var compressedMessageLength = writer.Position - initialPosition;
                    return Value.Count - compressedMessageLength;
                }
            }
        }

        private int WriteRecord(IKafkaWriter writer, MessageCodec codec, long firstOffset = 0L, DateTimeOffset? firstTimestamp = null)
        {
            var timestampDelta = firstTimestamp.HasValue && Timestamp.HasValue
                ? (long) Timestamp.Value.Subtract(firstTimestamp.Value).TotalMilliseconds
                : 0L;

            var compressionAmount = 0;
            using (writer.MarkForLength(varint: true)) {
                writer.Write((byte) 0) // attributes
                      .Write(timestampDelta, varint: true)
                      .Write(Math.Max(0L, Offset - firstOffset), varint: true)
                      .Write(Key, varint: true);
                if (codec == MessageCodec.None) {
                    writer.Write(Value, varint: true);
                } else {
                    using (writer.MarkForLength(varint: true)) {
                        var initialPosition = writer.Position;
                        writer.WriteCompressed(Value, codec);
                        var compressedMessageLength = writer.Position - initialPosition;
                        compressionAmount = Value.Count - compressedMessageLength;
                    }
                }
                writer.Write(Headers.Count); // TODO: varint length (?)
                foreach (var header in Headers) {
                    writer.Write(header.Key, varint: true)
                          .Write(header.Value, varint: true);
                }
            }
            return compressionAmount;
        }

        public Message(ArraySegment<byte> value, byte attribute, long offset = 0L, DateTimeOffset? timestamp = null)
            : this(value, EmptySegment, attribute, offset, timestamp)
        {
        }

        public Message(ArraySegment<byte> value, ArraySegment<byte> key, byte attribute, long offset = 0L, DateTimeOffset? timestamp = null, IEnumerable<MessageHeader> headers = null)
        {
            Offset = offset;
            Attribute = (byte)(attribute & MessageBatch.CodecMask);
            Key = key.Count > 0 ? key : EmptySegment;
            Value = value;
            Timestamp = timestamp;
            Headers = headers.ToSafeImmutableList();
        }

        /// <summary>
        /// Convenience constructor will encode both the key and message to byte streams.
        /// Most of the time a message will be string based.
        /// </summary>
        /// <param name="key">The key value for the message.  Can be null.</param>
        /// <param name="value">The main content data of this message.</param>
        public Message(string value, string key = null)
            : this(ToSegment(value), ToSegment(key), 0)
        {
        }

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        private static ArraySegment<byte> ToSegment(string value)
        {
            if (string.IsNullOrEmpty(value)) return EmptySegment;
            return new ArraySegment<byte>(Encoding.UTF8.GetBytes(value));
        }

        /// <summary>
        /// The log offset of this message as stored by the Kafka server.
        /// Version 0-1: When the producer is sending non compressed messages, it can set the offsets to anything. 
        ///              When the producer is sending compressed messages, to avoid server side recompression, each 
        ///              compressed message should have offset starting from 0 and increasing by one for each inner 
        ///              message in the compressed message.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// 
        /// The lowest 3 bits contain the compression codec used for the message.
        /// The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        /// All other bits should be set to 0.
        /// </summary>
        public byte Attribute { get; }

        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public ArraySegment<byte> Key { get; }

        /// <summary>
        /// The message body contents.  Can contain compressed message set.
        /// </summary>
        public ArraySegment<byte> Value { get; }

        /// <summary>
        /// This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        /// </summary>
        public DateTimeOffset? Timestamp { get; }

        /// <summary>
        /// Application level record level headers
        /// Version 2+ only.
        /// </summary>
        public IImmutableList<MessageHeader> Headers { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Message);
        }

        /// <inheritdoc />
        public bool Equals(Message other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offset == other.Offset 
                && Attribute == other.Attribute 
                && Key.HasEqualElementsInOrder(other.Key)
                && Value.HasEqualElementsInOrder(other.Value) 
                && Timestamp?.ToUnixTimeMilliseconds() == other.Timestamp?.ToUnixTimeMilliseconds()
                && Headers.HasEqualElementsInOrder(other.Headers);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Offset.GetHashCode();
                hashCode = (hashCode*397) ^ Attribute.GetHashCode();
                hashCode = (hashCode*397) ^ Key.Count.GetHashCode();
                hashCode = (hashCode*397) ^ Value.Count.GetHashCode();
                hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                hashCode = (hashCode*397) ^ Headers.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}