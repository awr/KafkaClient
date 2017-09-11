using KafkaClient.Assignment;
using KafkaClient.Protocol;

namespace KafkaClient.Tests
{
    public class ByteMembershipEncoder : MembershipEncoder<ByteTypeMetadata, ByteTypeAssignment>
    {
        /// <inheritdoc />
        public ByteMembershipEncoder(string protocolType) : base(protocolType)
        {
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ByteTypeMetadata value)
        {
            writer.Write(value.Bytes);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ByteTypeAssignment value)
        {
            writer.Write(value.Bytes);
        }

        protected override ByteTypeMetadata DecodeMetadata(string assignmentStrategy, IRequestContext context, IKafkaReader reader, int expectedLength)
        {
            var byteCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(byteCount, true);
            return new ByteTypeMetadata(assignmentStrategy, reader.ReadBytes(byteCount));
        }

        protected override ByteTypeAssignment DecodeAssignment(IRequestContext context, IKafkaReader reader, int expectedLength)
        {
            var byteCount = reader.ReadInt32();
            context.ThrowIfCountTooBig(byteCount, true);
            return new ByteTypeAssignment(reader.ReadBytes(byteCount));
        }
    }
}