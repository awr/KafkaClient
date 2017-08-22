using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Produce Request => *transactional_id acks timeout [topic_data] 
    /// </summary>
    /// <remarks>
    /// Produce Request => *transactional_id acks timeout [topic_data] 
    ///   transactional_id => NULLABLE_STRING
    ///   acks => INT16
    ///   timeout => INT32
    ///   topic_data => topic [data] 
    ///     topic => STRING
    ///     data => partition record_set 
    ///       partition => INT32
    ///       record_set => RECORDS
    /// 
    /// Version 3+: transactional_id
    /// From http://kafka.apache.org/protocol.html#The_Messages_Produce
    /// </remarks>
    public class ProduceRequest : Request, IRequest<ProduceResponse>, IEquatable<ProduceRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},acks:{Acks},timeout:{Timeout},transactional_id:{TransactionalId},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0].TopicName}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            var totalCompressedBytes = 0;
            var groupedPayloads = (from p in Topics
                                   group p by new {
                                       topic = p.TopicName, partition_id = p.PartitionId, p.Codec
                                   } into tpc
                                   select tpc).ToList();

            if (context.ApiVersion >= 3) {
                writer.Write(TransactionalId);
            }
            writer.Write(Acks)
                    .WriteMilliseconds(Timeout)
                    .Write(groupedPayloads.Count);

            byte version = 0;
            if (context.ApiVersion >= 2) {
                if (context.ApiVersion >= 3) {
                    version = 2;
                } else {
                    version = 1;
                }
            }

            foreach (var groupedPayload in groupedPayloads) {
                var topics = groupedPayload.ToList();
                writer.Write(groupedPayload.Key.topic)
                        .Write(topics.Count)
                        .Write(groupedPayload.Key.partition_id);

                // TODO: Add Txn related stuff to the context (?)
                var messageBatch = new MessageBatch(topics.SelectMany(x => x.Messages), groupedPayload.Key.Codec);
                var compressedBytes = messageBatch.WriteTo(writer, version);
                Interlocked.Add(ref totalCompressedBytes, compressedBytes);
            }

            if (context.OnProduceRequestMessages != null) {
                var segment = writer.ToSegment();
                context.OnProduceRequestMessages(Topics.Sum(_ => _.Messages.Count), segment.Count, totalCompressedBytes);
            }
        }

        public ProduceResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => ProduceResponse.FromBytes(context, bytes);

        public ProduceRequest(Topic topic, TimeSpan? timeout = null, short acks = 1, string transactionalId = null)
            : this(new [] { topic }, timeout, acks, transactionalId)
        {
        }

        public ProduceRequest(IEnumerable<Topic> payload, TimeSpan? timeout = null, short acks = 1, string transactionalId= null) 
            : base(ApiKey.Produce, acks != 0)
        {
            Timeout = timeout.GetValueOrDefault(TimeSpan.FromSeconds(1));
            Acks = acks;
            TransactionalId = transactionalId;
            Topics = payload != null ? payload.ToImmutableList() : ImmutableList<Topic>.Empty;
        }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// The transactional ID of the producer. This is used to authorize transaction produce requests. 
        /// This can be null for non-transactional producers.
        /// Version: 3+
        /// </summary>
        public string TransactionalId { get; }

        /// <summary>
        /// Level of ack required by kafka: 0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public short Acks { get; }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public IImmutableList<Topic> Topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceRequest);
        }

        /// <inheritdoc />
        public bool Equals(ProduceRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Timeout.Equals(other.Timeout) 
                && Acks == other.Acks 
                && TransactionalId == other.TransactionalId
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Timeout.GetHashCode();
                hashCode = (hashCode * 397) ^ Acks.GetHashCode();
                hashCode = (hashCode * 397) ^ (TransactionalId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        /// <summary>
        /// Buffer represents a collection of messages to be posted to a specified Topic on specified Partition.
        /// Included in <see cref="ProduceRequest"/>
        /// </summary>
        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},Codec:{Codec},Messages:{Messages.Count}}}";

            public Topic(string topicName, int partitionId, IEnumerable<Message> messages, MessageCodec codec = MessageCodec.None) 
                : base(topicName, partitionId)
            {
                Codec = codec;
                Messages = messages.ToSafeImmutableList();
            }

            public MessageCodec Codec { get; }
            public IImmutableList<Message> Messages { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            /// <inheritdoc />
            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && Codec == other.Codec 
                    && Messages.HasEqualElementsInOrder(other.Messages);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ (int) Codec;
                    hashCode = (hashCode*397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion

        }
    }
}