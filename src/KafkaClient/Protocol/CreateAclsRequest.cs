using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateAcls Request => [creations] 
    /// </summary>
    /// <remarks>
    /// CreateAcls Request => [creations] 
    ///   creations => resource_type resource_name principal host operation permission_type 
    ///     resource_type => INT8
    ///     resource_name => STRING
    ///     principal => STRING
    ///     host => STRING
    ///     operation => INT8
    ///     permission_type => INT8
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_CreateAcls
    /// </remarks>
    public class CreateAclsRequest : Request, IRequest<CreateAclsResponse>, IEquatable<CreateAclsRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},creations:[{Creations.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Creations.Count);
            foreach (var resource in Creations) {
                writer.Write(resource.ResourceType)
                      .Write(resource.ResourceName)
                      .Write(resource.Principal)
                      .Write(resource.Host)
                      .Write(resource.Operation)
                      .Write(resource.PermissionType);
            }
        }

        public CreateAclsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => CreateAclsResponse.FromBytes(context, bytes);

        public CreateAclsRequest(IEnumerable<AclResource> creations = null) 
            : base(ApiKey.CreateAcls)
        {
            Creations = creations.ToSafeImmutableList();
        }

        public IImmutableList<AclResource> Creations { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as CreateAclsRequest);
        }

        public bool Equals(CreateAclsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Creations.HasEqualElementsInOrder(other.Creations);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Creations.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}