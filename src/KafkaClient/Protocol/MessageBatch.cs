using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        public int WriteTo(IKafkaWriter writer, byte version)
        {
            // RecordBatch was introduced with version 2, and is significantly different from the MessageSet approach (see above)
            if (version >= 2) {
                return WriteRecordBatchTo(writer, version);
            } else {
                return WriteMessageSetTo(writer, version);
            }
        }

        private int WriteRecordBatchTo(IKafkaWriter writer, byte version)
        {
            (long firstOffset, short attributes, int lastOffsetDelta, long firstTimestamp, long maxTimestamp) = HarvestRecordBatchDetails(out DateTimeOffset? timestamp);

            void WriteUncompressedTo(IKafkaWriter messageWriter)
            {
                messageWriter.Write(Messages.Count);
                foreach (var message in Messages) {
                    message.WriteTo(messageWriter, version, firstOffset, timestamp);
                }
            }

            writer.Write(firstOffset);
            using (writer.MarkForLength()) {
                writer.Write(0) // PartitionLeaderEpoch int32
                      .Write(version); // AKA Magic

                // TODO: does the CRC only include the record data, or ... ?
                using (writer.MarkForCrc()) {
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

                    // TODO: how are the compressed records supposed to be written ??
                    using (var messageWriter = new KafkaWriter()) {
                        WriteUncompressedTo(messageWriter);
                        var records = messageWriter.ToSegment(false);
                        using (writer.MarkForLength()) { // messageset
                            var initialPosition = writer.Position;
                            writer.WriteCompressed(records, _codec);
                            var compressedMessageLength = writer.Position - initialPosition;
                            return records.Count - compressedMessageLength;
                        }
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
                firstOffset = Messages[0].Offset ?? firstOffset;
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
                    messageWriter.Write(index++);
                    using (messageWriter.MarkForLength()) {
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

                using (writer.MarkForLength()) { // messageset
                    writer.Write(0L); // offset
                    using (writer.MarkForLength()) { // message
                        using (writer.MarkForCrc()) {
                            writer.Write(version) // message version
                                  .Write((byte)_codec) // attribute
                                  .Write(-1); // key  -- null, so -1 length
                            using (writer.MarkForLength()) { // value
                                var initialPosition = writer.Position;
                                writer.WriteCompressed(messageSet, _codec);
                                var compressedMessageLength = writer.Position - initialPosition;
                                return messageSet.Count - compressedMessageLength;
                            }
                        }
                    }
                }
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