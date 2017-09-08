namespace KafkaClient.Protocol
{
    public interface IMessageContext
    {
        /// <summary>
        /// This is a numeric version number for the message (AKA magic). It allows the client and server to properly interpret the message as the protocol evolves.
        /// </summary>
        byte? MessageVersion { get; }

        /// <summary>
        /// This is the encoding to use for messages
        /// </summary>
        MessageCodec Codec { get; }
    }
}