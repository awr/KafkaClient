using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// AlterConfigs Response => throttle_time_ms [resources] 
    /// </summary>
    /// <remarks>
    /// AlterConfigs Response => throttle_time_ms [resources] 
    ///   throttle_time_ms => INT32
    ///   resources => error_code error_message resource_type resource_name 
    ///     error_code => INT16
    ///     error_message => NULLABLE_STRING
    ///     resource_type => INT8
    ///     resource_name => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_AlterConfigs
    /// </remarks>
    public class AlterConfigsResponse : ThrottledResponse, IResponse, IEquatable<AlterConfigsResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},resources:{Resources.ToStrings()}}}";

        public static AlterConfigsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();

                var resourceCount = reader.ReadInt32();
                reader.AssertMaxArraySize(resourceCount);
                var resources = new ConfigResource[resourceCount];
                for (var r = 0; r < resourceCount; r++ ) {
                    var errorCode = reader.ReadErrorCode();
                    var errorMessage = reader.ReadString();
                    var resourceType = reader.ReadByte();
                    var resourceName = reader.ReadString();
                    resources[r] = new ConfigResource(errorCode, errorMessage, resourceType, resourceName);
                }
                return new AlterConfigsResponse(throttleTime.Value, resources);
            }
        }

        public AlterConfigsResponse(TimeSpan throttleTime, IEnumerable<ConfigResource> resources = null)
            : base(throttleTime)
        {
            Resources = resources.ToSafeImmutableList();
            Errors = Resources.Select(r => r.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The resources and their associated ACLs.
        /// </summary>
        public IImmutableList<ConfigResource> Resources { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as AlterConfigsResponse);
        }

        /// <inheritdoc />
        public bool Equals(AlterConfigsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Resources.HasEqualElementsInOrder(other.Resources);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Resources.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}