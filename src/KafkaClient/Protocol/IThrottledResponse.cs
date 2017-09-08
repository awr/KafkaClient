using System;

namespace KafkaClient.Protocol
{
    public interface IThrottledResponse
    {
        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) 
        /// </summary>
        TimeSpan? ThrottleTime { get; }
    }
}