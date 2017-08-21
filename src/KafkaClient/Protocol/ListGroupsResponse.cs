using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ListGroups Response => *throttle_time_ms error_code [groups] 
    /// </summary>
    /// <remarks>
    /// ListGroups Response => *throttle_time_ms error_code [groups] 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   groups => group_id protocol_type 
    ///     group_id => STRING
    ///     protocol_type => STRING
    /// 
    /// Version 1+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_ListGroups
    /// </remarks>
    public class ListGroupsResponse : ThrottledResponse, IResponse, IEquatable<ListGroupsResponse>
    {
        public override string ToString() => $"{{error_code:{Error},groups:[{Groups.ToStrings()}]}}";

        public static ListGroupsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                var errorCode = (ErrorCode)reader.ReadInt16();
                var groups = new Group[reader.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var groupId = reader.ReadString();
                    var protocolType = reader.ReadString();
                    groups[g] = new Group(groupId, protocolType);
                }

                return new ListGroupsResponse(errorCode, groups, throttleTime);
            }
        }

        public ListGroupsResponse(ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Group> groups = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            Groups = groups.ToSafeImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode Error { get; }

        public IImmutableList<Group> Groups { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ListGroupsResponse);
        }

        /// <inheritdoc />
        public bool Equals(ListGroupsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && Error == other.Error
                && Groups.HasEqualElementsInOrder(other.Groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Groups?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{group_id:{GroupId},protocol_type:{ProtocolType}}}";

            public Group(string groupId, string protocolType)
            {
                GroupId = groupId;
                ProtocolType = protocolType;
            }

            /// <summary>
            /// The group id.
            /// </summary>
            public string GroupId { get; }

            /// <summary>
            /// Unique name for class of protocols implemented by group.
            /// </summary>
            public string ProtocolType { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Group);
            }

            /// <inheritdoc />
            public bool Equals(Group other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(GroupId, other.GroupId) 
                       && string.Equals(ProtocolType, other.ProtocolType);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((GroupId?.GetHashCode() ?? 0)*397) ^ (ProtocolType?.GetHashCode() ?? 0);
                }
            }
            
            #endregion
        }
    }
}