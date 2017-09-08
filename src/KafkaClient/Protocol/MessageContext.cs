namespace KafkaClient.Protocol
{
    public class MessageContext : IMessageContext
    {
        public MessageContext(MessageCodec codec = MessageCodec.None, byte? version = null)
        {
            Codec = codec;
            MessageVersion = version;
        }

        public byte? MessageVersion { get; }
        public MessageCodec Codec { get; }
    }
}