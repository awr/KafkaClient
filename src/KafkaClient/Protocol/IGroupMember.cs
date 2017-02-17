// ReSharper disable InconsistentNaming
namespace KafkaClient.Protocol
{
    public interface IGroupMember
    {
        /// <summary>
        /// The group id.
        /// </summary>
        string group_id { get; }

        /// <summary>
        /// The member id assigned by the group coordinator (ie one of the kafka servers).
        /// In the join group phase, this is empty (ie "") for first timers, but rejoining members should use the same memberId (from previous generations).
        /// </summary>
        string member_id { get; }
    }
}