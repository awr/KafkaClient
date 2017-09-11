using System.Collections.Generic;
using System.Linq;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public class ConsumerEncoder : MembershipEncoder<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        public const string Protocol = "consumer";

        public ConsumerEncoder(params IMembershipAssignor[] assignors) : this((IEnumerable<IMembershipAssignor>)assignors)
        {
        }

        /// <inheritdoc />
        public ConsumerEncoder(IEnumerable<IMembershipAssignor> assignors) : base(Protocol, assignors ?? SimpleAssignor.Assignors)
        {
        }

        /// <inheritdoc />
        protected override ConsumerProtocolMetadata DecodeMetadata(string assignmentStrategy, IRequestContext context, IKafkaReader reader, int expectedLength)
        {
            var version = reader.ReadInt16();
            var topicCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(topicCount);
            var topicNames = new string[topicCount];
            for (var t = 0; t < topicNames.Length; t++) {
                topicNames[t] = reader.ReadString();
            }
            var byteCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(byteCount, true);
            var userData = reader.ReadBytes(byteCount);
            return new ConsumerProtocolMetadata(topicNames, assignmentStrategy, userData, version);
        }

        /// <inheritdoc />
        protected override ConsumerMemberAssignment DecodeAssignment(IRequestContext context, IKafkaReader reader, int expectedLength)
        {
            var version = reader.ReadInt16();

            var topicCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(topicCount);
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicCount; t++) {
                var topicName = reader.ReadString();

                var partitionCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(partitionCount);
                for (var p = 0; p < partitionCount; p++) {
                    var partitionId = reader.ReadInt32();
                    topics.Add(new TopicPartition(topicName, partitionId));
                }
            }
            var byteCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(byteCount, true);
            var userData = reader.ReadBytes(byteCount);
            return new ConsumerMemberAssignment(topics, userData, version);
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ConsumerProtocolMetadata value)
        {
            writer.Write(value.Version)
                  .Write(value.Subscriptions, true)
                  .Write(value.UserData);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ConsumerMemberAssignment value)
        {
            writer.Write(value.Version)
                  .WriteGroupedTopics(value.PartitionAssignments)
                  .Write(value.UserData);
        }
    }
}