using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteAcls Request => [filters] 
    /// </summary>
    /// <remarks>
    /// DeleteAcls Request => [filters] 
    ///   filters => resource_type resource_name principal host operation permission_type 
    ///     resource_type => INT8
    ///     resource_name => NULLABLE_STRING
    ///     principal => NULLABLE_STRING
    ///     host => NULLABLE_STRING
    ///     operation => INT8
    ///     permission_type => INT8
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DeleteAcls
    /// </remarks>
    public class DeleteAclsRequest : Request, IRequest<DeleteAclsResponse>, IEquatable<DeleteAclsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},creations:[{Filters.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Filters.Count);
            foreach (var resource in Filters) {
                writer.Write(resource.ResourceType)
                      .Write(resource.ResourceName)
                      .Write(resource.Principal)
                      .Write(resource.Host)
                      .Write(resource.Operation)
                      .Write(resource.PermissionType);
            }
        }

        public DeleteAclsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DeleteAclsResponse.FromBytes(context, bytes);

        public DeleteAclsRequest(IEnumerable<AclResource> filters = null) 
            : base(ApiKey.DeleteAcls)
        {
            Filters = ImmutableList<AclResource>.Empty.AddNotNullRange(filters);
        }

        public IImmutableList<AclResource> Filters { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteAclsRequest);
        }

        public bool Equals(DeleteAclsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Filters.HasEqualElementsInOrder(other.Filters);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Filters.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}