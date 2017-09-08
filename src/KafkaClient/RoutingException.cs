using System;

namespace KafkaClient
{
    /// <summary>
    /// An exception cause by invalid/missing/out-of-date metadata in the local metadata cache, where requests cannot be routed to a broker (or are routed to the wrong broker)
    /// </summary>
    public class RoutingException : KafkaException
    {
        public RoutingException(string message)
            : base(message)
        {
        }

        public RoutingException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}