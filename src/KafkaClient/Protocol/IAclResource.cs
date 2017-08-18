namespace KafkaClient.Protocol
{
    public interface IAclResource : IAcl
    {
        /// <summary>
        /// The resource type (filter)
        /// </summary>
        byte ResourceType { get; }

        /// <summary>
        /// The resource name (filter)
        /// </summary>
        string ResourceName { get; }
    }
}