using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteAcls Response => throttle_time_ms [filter_responses] 
    /// </summary>
    /// <remarks>
    /// DeleteAcls Response => throttle_time_ms [filter_responses] 
    ///   throttle_time_ms => INT32
    ///   filter_responses => error_code error_message [matching_acls] 
    ///     error_code => INT16
    ///     error_message => NULLABLE_STRING
    ///     matching_acls => error_code error_message resource_type resource_name principal host operation permission_type 
    ///       error_code => INT16
    ///       error_message => NULLABLE_STRING
    ///       resource_type => INT8
    ///       resource_name => STRING
    ///       principal => STRING
    ///       host => STRING
    ///       operation => INT8
    ///       permission_type => INT8
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DeleteAcls
    /// </remarks>
    public class DeleteAclsResponse : ThrottledResponse, IResponse, IEquatable<DeleteAclsResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},filter_responses:{FilterResponses.ToStrings()}}}";

        public static DeleteAclsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();

                var filterCount = reader.ReadInt32();
                reader.AssertMaxArraySize(filterCount);
                var filters = new FilterResponse[filterCount];
                for (var r = 0; r < filterCount; r++ ) {
                    var errorCode = (ErrorCode) reader.ReadInt16();
                    var errorMessage = reader.ReadString();
                    var aclCount = reader.ReadInt32();
                    reader.AssertMaxArraySize(aclCount);
                    var acls = new MatchingAcl[aclCount];
                    for (var a = 0; a < acls.Length; a++) {
                        var aclErrorCode = (ErrorCode) reader.ReadInt16();
                        var aclErrorMessage = reader.ReadString();
                        var resourceType = reader.ReadByte();
                        var resourceName = reader.ReadString();
                        var principal = reader.ReadString();
                        var host = reader.ReadString();
                        var operation = reader.ReadByte();
                        var permissionType = reader.ReadByte();
                        acls[a] = new MatchingAcl(aclErrorCode, aclErrorMessage, resourceType, resourceName, principal, host, operation, permissionType);
                    }
                    filters[r] = new FilterResponse(errorCode, errorMessage, acls);
                }
                return new DeleteAclsResponse(throttleTime.Value, filters);
            }
        }

        public DeleteAclsResponse(TimeSpan throttleTime, IEnumerable<FilterResponse> filterResponses = null)
            : base(throttleTime)
        {
            FilterResponses = filterResponses.ToSafeImmutableList();
            Errors = ImmutableList<ErrorCode>.Empty
                .AddRange(FilterResponses.Select(r => r.Error))
                .AddRange(FilterResponses.SelectMany(r => r.MatchingAcls.Select(m => m.Error)));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<FilterResponse> FilterResponses { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteAclsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DeleteAclsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && FilterResponses.HasEqualElementsInOrder(other.FilterResponses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ FilterResponses.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class FilterResponse : IEquatable<FilterResponse>
        {
            public override string ToString() => $"{{error_code:{Error},error_message:{ErrorMessage},matching_acls:[{MatchingAcls.ToStrings()}]}}";

            public FilterResponse(ErrorCode errorCode, string errorMessage, IEnumerable<MatchingAcl> matchingAcls = null)
            {
                Error = errorCode;
                ErrorMessage = errorMessage;
                MatchingAcls = matchingAcls.ToSafeImmutableList();
            }

            /// <summary>
            /// The error code.
            /// </summary>
            public ErrorCode Error { get; }

            /// <summary>
            /// The error message.
            /// </summary>
            public string ErrorMessage { get; }

            public IImmutableList<MatchingAcl> MatchingAcls { get; }
 
            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as FilterResponse);
            }

            /// <inheritdoc />
            public bool Equals(FilterResponse other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Error == other.Error
                    && string.Equals(ErrorMessage, other.ErrorMessage)
                    && MatchingAcls.HasEqualElementsInOrder(other.MatchingAcls);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = Error.GetHashCode();
                    hashCode = (hashCode*397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ MatchingAcls.Count.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }

        public class MatchingAcl : AclResource, IEquatable<MatchingAcl>
        {
            public override string ToString() => $"{{error_code:{Error},error_message:{ErrorMessage},{this.AclToString()}}}";

            public MatchingAcl(ErrorCode errorCode, string errorMessage, byte resourceType, string resourceName, string principal, string host, byte operation, byte permissionType)
                : base(resourceType, resourceName, principal, host, operation, permissionType)
            {
                Error = errorCode;
                ErrorMessage = errorMessage;
            }

            /// <summary>
            /// The error code.
            /// </summary>
            public ErrorCode Error { get; }

            /// <summary>
            /// The error message.
            /// </summary>
            public string ErrorMessage { get; }
            
            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as MatchingAcl);
            }

            /// <inheritdoc />
            public bool Equals(MatchingAcl other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other)
                    && Error == other.Error
                    && string.Equals(ErrorMessage, other.ErrorMessage);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Error.GetHashCode();
                    hashCode = (hashCode*397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}