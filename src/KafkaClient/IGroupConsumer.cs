namespace KafkaClient
{
    public interface IGroupConsumer : IConsumer
    {
        string GroupId { get; }
        string MemberId { get; }
        int GenerationId { get; }
        bool IsLeader { get; }
        string ProtocolType { get; }
    }
}