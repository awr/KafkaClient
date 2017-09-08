using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// InitProducerId Response => throttle_time_ms error_code producer_id producer_epoch 
    /// </summary>
    /// <remarks>
    /// InitProducerId Response => throttle_time_ms error_code producer_id producer_epoch 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   producer_id => INT64
    ///   producer_epoch => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_InitProducerId
    /// </remarks>
    public class InitProducerIdResponse : ThrottledResponse, IResponse, IEquatable<InitProducerIdResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},error_code:{Error},producer_id:{ProducerId},producer_epoch:{ProducerEpoch}}}";

        public static InitProducerIdResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();
                var errorCode = reader.ReadErrorCode();
                var producerId = reader.ReadInt64();
                var producerEpoch = reader.ReadInt16();

                return new InitProducerIdResponse(throttleTime.Value, errorCode, producerId, producerEpoch);
            }
        }

        public InitProducerIdResponse(TimeSpan throttleTime, ErrorCode errorCode, long producerId, short producerEpoch)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            ProducerId = producerId;
            ProducerEpoch = producerEpoch;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode Error { get; }

        /// <summary>
        /// 	The producer id for the input transactional id. If the input id was empty, then this is used only for ensuring idempotence of messages.
        /// </summary>
        public long ProducerId { get; }

        /// <summary>
        /// The epoch for the producer id. Will always be 0 if no transactional id was specified in the request.
        /// </summary>
        public short ProducerEpoch { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as InitProducerIdResponse);
        }

        /// <inheritdoc />
        public bool Equals(InitProducerIdResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error
                && ProducerId == other.ProducerId
                && ProducerEpoch == other.ProducerEpoch;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Error.GetHashCode();
                hashCode = (hashCode*397) ^ ProducerId.GetHashCode();
                hashCode = (hashCode*397) ^ ProducerEpoch.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}