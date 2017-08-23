using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// RecordBatch => FirstOffset Length PartitionLeaderEpoch Magic CRC Attributes LastOffsetDelta FirstTimestamp MaxTimestamp ProducerId ProducerEpoch FirstSequence [Record]
    /// </summary>
    /// <remarks>
    /// Version 2+:
    /// RecordBatch => FirstOffset Length PartitionLeaderEpoch Magic CRC Attributes LastOffsetDelta FirstTimestamp MaxTimestamp ProducerId ProducerEpoch FirstSequence [Record]
    ///   FirstOffset => int64
    ///   Length => int32
    ///   PartitionLeaderEpoch => int32
    ///   Magic => int8 
    ///   CRC => int32
    ///   Attributes => int16
    ///   LastOffsetDelta => int32
    ///   FirstTimestamp => int64
    ///   MaxTimestamp => int64
    ///   ProducerId => int64
    ///   ProducerEpoch => int16
    ///   FirstSequence => int32
    ///   Records => [Record]
    /// 
    /// PartitionLeaderEpoch is set by the broker upon receipt of a produce request and is used to ensure no loss of data when there are leader changes with log truncation. 
    ///   Client developers do not need to worry about setting this value.
    /// 
    /// Version 0-1:
    /// MessageSet => [Offset MessageSize Message]
    ///   Offset => int64
    ///   MessageSize => int32
    /// 
    /// MessageSets are not preceded by an int32 like other array elements in the protocol.
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </remarks>
    public class MessageBatch
    {
        /// <summary>
        /// Encodes a batch of messages in a writer
        /// </summary>
        public int WriteTo(IKafkaWriter writer, byte version)
        {
            // RecordBatch was introduced with version 2, and is significantly different from the MessageSet approach (see above)
            return version >= 2
                ? WriteRecordBatchTo(writer, version)
                : WriteMessageSetTo(writer, version);
        }

        private int WriteRecordBatchTo(IKafkaWriter writer, byte version)
        {
            (long firstOffset, short attributes, int lastOffsetDelta, long firstTimestamp, long maxTimestamp) = HarvestRecordBatchDetails(out DateTimeOffset? timestamp);

            void WriteUncompressedTo(IKafkaWriter messageWriter)
            {
                messageWriter.Write(Messages.Count);
                foreach (var message in Messages) {
                    message.WriteTo(messageWriter, version, _codec, firstOffset, timestamp);
                }
            }

            writer.Write(firstOffset);
            using (writer.MarkForLength()) {
                writer.Write(0) // PartitionLeaderEpoch int32
                      .Write(version); // AKA Magic

                // the CRC includes everything from the attribute on
                using (writer.MarkForCrc(castagnoli: true)) {
                    writer.Write(attributes)
                          .Write(lastOffsetDelta)
                          .Write(firstTimestamp)
                          .Write(maxTimestamp)
                          .Write(ProducerId)
                          .Write(ProducerEpoch)
                          .Write(FirstSequence);

                    if (_codec == MessageCodec.None) {
                        WriteUncompressedTo(writer);
                        return 0;
                    }

                    using (var messageWriter = new KafkaWriter()) {
                        WriteUncompressedTo(messageWriter);
                        var recordsValue = messageWriter.ToSegment(false);
                        var compressedMessage = new Message(recordsValue, (byte)_codec, firstOffset);

                        writer.Write(1); // record count = 1 for compressed set
                        return compressedMessage.WriteTo(writer, version, _codec);
                    }                    
                }
            }
        }

        private (long firstOffset, short attributes, int lastOffsetDelta, long firstTimestamp, long maxTimestamp) HarvestRecordBatchDetails(out DateTimeOffset? timestamp)
        {
            // Denotes the first offset in the RecordBatch. The 'offsetDelta' of each Record in the batch would be be computed relative to this FirstOffset. 
            // In particular, the offset of each Record in the Batch is its 'OffsetDelta' + 'FirstOffset'.
            var firstOffset = 0L;

            // Attributes int16
            // The lowest 3 bits contain the compression codec used for the message.
            // The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
            // The fifth lowest bit indicates whether the RecordBatch is part of a transaction or not. 0 indicates that the RecordBatch is not transactional, while 1 indicates that it is. (since 0.11.0.0)
            // 
            // The sixth lowest bit indicates whether the RecordBatch includes a control message. 1 indicates that the RecordBatch is contains a control message, 0 indicates that it doesn't. 
            // Control messages are used to enable transactions in Kafka and are generated by the broker. Clients should not return control batches (ie. those with this bit set) to applications. (since 0.11.0.0)
            // 
            // All other bits should be set to 0.
            var attributes = (short) (CodecMask & (short) _codec);
            if (ProducerId > 0L && ProducerEpoch > 0) {
                attributes = (short) (attributes | IsTransactionMask);
            }

            // The timestamp of the first Record in the batch. The timestamp of each Record in the RecordBatch is its 'TimestampDelta' + 'FirstTimestamp'.
            var firstTimestamp = 0L;

            // The offset of the last message in the RecordBatch. This is used by the broker to ensure correct behavior even when Records within a batch are compacted out.
            var lastOffsetDelta = Messages.Count;
            timestamp = null;

            // The timestamp of the last Record in the batch. This is used by the broker to ensure the correct behavior even when Records within the batch are compacted out.
            var maxTimestamp = 0L;
            if (Messages.Count > 0) {
                firstOffset = Messages[0].Offset;
                timestamp = Messages[0].Timestamp.GetValueOrDefault(DateTimeOffset.UtcNow);
                var first = timestamp.GetValueOrDefault(DateTimeOffset.UtcNow);
                firstTimestamp = first.ToUnixTimeMilliseconds();
                maxTimestamp = Messages.Where(m => m.Timestamp.HasValue).Max(m => m.Timestamp)
                                       .GetValueOrDefault(first).ToUnixTimeMilliseconds();
            }

            return (firstOffset, attributes, lastOffsetDelta, firstTimestamp, maxTimestamp);
        }

        private int WriteMessageSetTo(IKafkaWriter writer, byte version)
        {
            void WriteUncompressedTo(IKafkaWriter messageWriter)
            {
                var index = 0L;
                foreach (var message in Messages) {
                    messageWriter.Write(index); // offset does not increase, even though docs claim it does ...
                    using (messageWriter.MarkForLength()) { // message length
                        message.WriteTo(messageWriter, version);
                    }
                }
            }

            if (_codec == MessageCodec.None) {
                using (writer.MarkForLength()) {
                    WriteUncompressedTo(writer);
                }
                return 0;
            }
            using (var messageWriter = new KafkaWriter()) {
                WriteUncompressedTo(messageWriter);
                var messageSet = messageWriter.ToSegment(false);
                var compressedMessage = new Message(messageSet, (byte)_codec);

                using (writer.MarkForLength()) { // messageset
                    writer.Write(0L); // offset
                    using (writer.MarkForLength()) { // message length
                        return compressedMessage.WriteTo(writer, version, _codec);
                    }
                }
            }
        }

        /// <summary>
        /// Decode a byte[] that represents a batch of messages.
        /// </summary>
        public static MessageBatch ReadFrom(IKafkaReader reader)
        {
            (long offset, int length, uint crc, byte version, uint crcHash, int expectedLength) = ReadMessageHeader(reader);
            if (version >= 2) {
                return ReadRecordBatchFrom(reader, offset, length, (int)crc, version);
            } else {
                var messages = ReadMessageSetFrom(reader, MessageCodec.None, reader.Position + expectedLength, offset, length, crc, crcHash, version);
                return new MessageBatch(messages);
            }
        }

        private static (long offset, int length, uint crc, byte version, uint crcHash, int expectedLength) ReadMessageHeader(IKafkaReader reader, MessageCodec codec = MessageCodec.None)
        {
            var expectedLength = reader.ReadInt32(); // MessageSet size
            if (!reader.HasBytes(expectedLength)) throw new BufferUnderRunException($"Message set size of {expectedLength} is not fully available (codec {codec}).");

            var offset = reader.ReadInt64();
            var length = reader.ReadInt32();
            var crc = reader.ReadUInt32(); // RecordBatch this is the PartitionLeaderEpoch
            var crcHash = reader.ReadCrc(length - 4);
            var version = reader.ReadByte();

            return (offset, length, crc, version, crcHash, expectedLength - 17);
        }

        private static MessageBatch ReadRecordBatchFrom(IKafkaReader reader, long firstOffset, int length, int partitionLeaderEpoch, byte version)
        {
            var crc = reader.ReadUInt32();
            var crcHash = reader.ReadCrc(length - 9, castagnoli: true);
            if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

            var attributes = reader.ReadInt16();
            var lastOffsetDelta = reader.ReadInt32();
            var firstTimestampMilliseconds = reader.ReadInt64();
            var maxTimestamp = reader.ReadInt64();
            var producerId = reader.ReadInt64();
            var producerEpoch = reader.ReadInt16();
            var firstSequence = reader.ReadInt32();

            var firstTimestamp = firstTimestampMilliseconds >= 0 ? (DateTimeOffset?)DateTimeOffset.FromUnixTimeMilliseconds(firstTimestampMilliseconds) : null;
            var codec = (MessageCodec) (attributes & CodecMask);
            var messages = ReadRecords(reader, firstOffset, firstTimestamp, codec);

            return new MessageBatch(messages, codec, producerId, producerEpoch, firstSequence);
        }

        private static IEnumerable<Message> ReadRecords(IKafkaReader reader, long firstOffset, DateTimeOffset? firstTimestamp, MessageCodec codec)
        {
            var messages = new List<Message>();
            var messageCount = reader.ReadInt32();

            for (var m = 0; m < messageCount; m++) {
                var length = reader.ReadInt32(varint: true);
                if (!reader.HasBytes(length)) throw new BufferUnderRunException($"Record size of {length} is not fully available (codec {codec}).");
                var attribute = reader.ReadByte();
                var timestampDelta = reader.ReadInt64(varint: true);
                var offsetDelta = reader.ReadInt64(varint: true);
                var key = reader.ReadBytes(varint: true);
                var value = reader.ReadBytes(varint: true);

                MessageHeader[] headers = null;
                var headerCount = reader.ReadInt32();
                if (headerCount > 0) {
                    headers = new MessageHeader[headerCount];
                    for (var h = 0; h < headerCount; h++) {
                        var headerKey = reader.ReadString(varint: true);
                        var headerValue = reader.ReadBytes(varint: true);
                        headers[h] = new MessageHeader(headerKey, headerValue);
                    }
                }

                var timestamp = firstTimestamp?.Add(TimeSpan.FromMilliseconds(timestampDelta));

                if (codec == MessageCodec.None) {
                    messages.Add(new Message(value, key, attribute, firstOffset + offsetDelta, timestamp, headers));
                } else {
                    var uncompressedBytes = value.ToUncompressed(codec);
                    using (var messageRecordsReader = new KafkaReader(uncompressedBytes)) {
                        messages.AddRange(ReadRecords(messageRecordsReader, firstOffset, firstTimestamp, MessageCodec.None));
                    }
                }
            }
            return messages;
        }

        private static IEnumerable<Message> ReadMessageSetFrom(IKafkaReader reader, MessageCodec codec, long finalPosition, long offset, int length, uint crc, uint crcHash, byte version)
        {
            var messages = new List<Message>();
            while (reader.Position < finalPosition) {
                //if (!offset.HasValue) {
                //    if (reader.HasBytes(12)) break;
                //    offset = reader.ReadInt64();
                //}
                //if (!length.HasValue) {
                //    // Won't have offset but not length
                //    length = reader.ReadInt32();
                //}

                // if the stream does not have enough left in the payload, we got only a partial message
                if (!reader.HasBytes(length)) throw new BufferUnderRunException($"Message size of {length} is not fully available (codec {codec}).");

                try {
                    messages.AddRange(ReadMessage(reader, offset, crc, crcHash, version));
                } catch (EndOfStreamException ex) {
                    throw new BufferUnderRunException($"Message size of {length} is not available (codec {codec}).", ex);
                }
            }
            return messages;
        }

        /// <summary>
        /// Decode messages from a reader.
        /// </summary>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        private static IEnumerable<Message> ReadMessage(IKafkaReader reader, long offset, uint crc, uint crcHash, byte version)
        {
            //if (!crc.HasValue) {
            //    crc = reader.ReadUInt32();
            //}
            //if (!crcHash.HasValue) {
            //    crcHash = reader.ReadCrc(length - 4);    
            //}
            
            if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

            //if (!version.HasValue) {
            //    version = reader.ReadByte();
            //}
            var attribute = reader.ReadByte();
            DateTimeOffset? timestamp = null;
            if (version >= 1) {
                var milliseconds = reader.ReadInt64();
                if (milliseconds >= 0) {
                    timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                }
            }
            var key = reader.ReadBytes();
            var value = reader.ReadBytes();

            var codec = (MessageCodec)(CodecMask & attribute);
            if (codec == MessageCodec.None) {
                return new [] { new Message(value, key, attribute, offset, timestamp) };
            }
            var uncompressedBytes = value.ToUncompressed(codec);
            using (var messageSetReader = new KafkaReader(uncompressedBytes)) {
                int length, expectedLength;
                (offset, length, crc, version, crcHash, expectedLength) = ReadMessageHeader(reader, codec);
                return ReadMessageSetFrom(messageSetReader, codec, reader.Position + expectedLength, offset, length, crc, crcHash, version);
            }
        }

        public MessageBatch(IEnumerable<Message> messages, MessageCodec codec = MessageCodec.None, long producerId = 0L, short producerEpoch = 0, int sequence = 0)
        {
            Messages = messages.ToImmutableList();
            ProducerId = producerId;
            ProducerEpoch = producerEpoch;
            FirstSequence = sequence;
            _codec = codec;
        }

        private readonly MessageCodec _codec;

        /// <summary>
        ///  The lowest 3 bits contain the compression codec used for the message. The other bits should be set to 0.
        /// </summary>
        public const byte CodecMask = 0x7;

        /// <summary>
        /// The fifth lowest bit indicates whether the RecordBatch is part of a transaction or not. 0 indicates that the RecordBatch is not transactional, while 1 indicates that it is. (since 0.11.0.0)
        /// </summary>
        public const byte IsTransactionMask = 0x10; 

        /// <summary>
        ///  The broker assigned producerId received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
        /// </summary>
        public long ProducerId { get; }

        /// <summary>
        /// The broker assigned producerEpoch received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
        /// </summary>
        public short ProducerEpoch { get; }

        /// <summary>
        /// The producer assigned sequence number which is used by the broker to deduplicate messages. Clients which want to support idempotent message delivery and transactions must set this field. 
        /// The sequence number for each Record in the RecordBatch is its OffsetDelta + FirstSequence.
        /// </summary>
        public int FirstSequence { get; }

        public IImmutableList<Message> Messages { get; }
    }
}