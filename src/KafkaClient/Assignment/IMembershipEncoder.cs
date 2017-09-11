using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public interface IMembershipEncoder
    {
        string ProtocolType { get; }

        void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value);
        void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value);

        IMemberMetadata DecodeMetadata(string protocol, IRequestContext context, IKafkaReader reader);
        IMemberAssignment DecodeAssignment(IRequestContext context, IKafkaReader reader);

        IMembershipAssignor GetAssignor(string strategy);
    }
}