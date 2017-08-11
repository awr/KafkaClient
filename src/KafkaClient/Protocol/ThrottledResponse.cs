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
    public abstract class ThrottledResponse : IThrottledResponse
    {
        protected ThrottledResponse(TimeSpan? throttleTime)
        {
            ThrottleTime = throttleTime;
        }

        /// <inheritdoc />
        public TimeSpan? ThrottleTime { get; }

        /// <inheritdoc />
        protected bool Equals(ThrottledResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return (int?) ThrottleTime?.TotalMilliseconds == (int?) other.ThrottleTime?.TotalMilliseconds;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ThrottleTime?.TotalMilliseconds.GetHashCode() ?? 0;
            }
        }
    }
}