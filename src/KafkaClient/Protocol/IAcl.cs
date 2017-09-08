namespace KafkaClient.Protocol
{
    public interface IAcl
    {
        /// <summary>
        /// The ACL principal (filter)
        /// </summary>
        string Principal { get; }

        /// <summary>
        /// The ACL host (filter)
        /// </summary>
        string Host { get; }

        /// <summary>
        /// The ACL operation (filter)
        /// </summary>
        byte Operation { get; }

        /// <summary>
        /// The ACL permission type (filter)
        /// </summary>
        byte PermissionType { get; }
    }
}