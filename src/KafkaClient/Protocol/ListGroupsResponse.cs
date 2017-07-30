using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ListGroups Response => error_code [groups]
    ///   error_code => INT16
    ///   group => [group_id protocol_type]
    ///     group_id => STRING
    ///     protocol_type => STRING
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class ListGroupsResponse : IResponse, IEquatable<ListGroupsResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},groups:[{groups.ToStrings()}]}}";

        public static ListGroupsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var groups = new Group[reader.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var groupId = reader.ReadString();
                    var protocolType = reader.ReadString();
                    groups[g] = new Group(groupId, protocolType);
                }

                return new ListGroupsResponse(errorCode, groups);
            }
        }

        public ListGroupsResponse(ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Group> groups = null)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
            this.groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode error_code { get; }

        public IImmutableList<Group> groups { get; }

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
            return error_code == other.error_code
                   && groups.HasEqualElementsInOrder(other.groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) error_code*397) ^ (groups?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{group_id:{group_id},protocol_type:{protocol_type}}}";

            public Group(string groupId, string protocolType)
            {
                group_id = groupId;
                protocol_type = protocolType;
            }

            public string group_id { get; }
            public string protocol_type { get; }

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
                return string.Equals(group_id, other.group_id) 
                       && string.Equals(protocol_type, other.protocol_type);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((group_id?.GetHashCode() ?? 0)*397) ^ (protocol_type?.GetHashCode() ?? 0);
                }
            }
            
            #endregion
        }
    }
}