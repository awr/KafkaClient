using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeAcls Response => throttle_time_ms error_code error_message [resources] 
    /// </summary>
    /// <remarks>
    /// DescribeAcls Response => throttle_time_ms error_code error_message [resources] 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   error_message => NULLABLE_STRING
    ///   resources => resource_type resource_name [acls] 
    ///     resource_type => INT8
    ///     resource_name => STRING
    ///     acls => principal host operation permission_type 
    ///       principal => STRING
    ///       host => STRING
    ///       operation => INT8
    ///       permission_type => INT8
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
    /// </remarks>
    public class DescribeAclsResponse : ThrottledResponse, IResponse, IEquatable<DescribeAclsResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{ThrottleTime},error:{Error}}}";

        public static DescribeAclsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();
                var errorCode = (ErrorCode) reader.ReadInt16();
                var errorMessage = reader.ReadString();

                var resourceCount = reader.ReadInt32();
                var resources = new List<AclResource>();
                for (var r = 0; r < resourceCount; r++ ) {
                    var resourceType = reader.ReadByte();
                    var resourceName = reader.ReadString();
                    var aclCount = reader.ReadInt32();
                    for (var a = 0; a < aclCount; a++) {
                        var principal = reader.ReadString();
                        var host = reader.ReadString();
                        var operation = reader.ReadByte();
                        var permissionType = reader.ReadByte();

                        resources.Add(new AclResource(resourceType, resourceName, principal, host, operation, permissionType));
                    }
                }
                return new DescribeAclsResponse(throttleTime.Value, errorCode, errorMessage, resources);
            }
        }

        public DescribeAclsResponse(TimeSpan throttleTime, ErrorCode errorCode, string errorMessage, IEnumerable<AclResource> resources = null)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            ErrorMessage = errorMessage;
            Resources = resources.ToSafeImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode Error { get; }

        /// <summary>
        /// The error message.
        /// </summary>
        public string ErrorMessage { get; }

        /// <summary>
        /// The resources and their associated ACLs.
        /// </summary>
        public IImmutableList<AclResource> Resources { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeAclsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DescribeAclsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error
                && string.Equals(ErrorMessage, other.ErrorMessage)
                && Resources.HasEqualElementsInOrder(other.Resources);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Error.GetHashCode();
                hashCode = (hashCode*397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Resources.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}