using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeAcls Request => resource_type resource_name principal host operation permission_type 
    /// </summary>
    /// <remarks>
    /// DescribeAcls Request => resource_type resource_name principal host operation permission_type 
    ///   resource_type => INT8
    ///   resource_name => NULLABLE_STRING
    ///   principal => NULLABLE_STRING
    ///   host => NULLABLE_STRING
    ///   operation => INT8
    ///   permission_type => INT8
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeAcls
    /// </remarks>
    public class DescribeAclsRequest : Request, IAclResource, IRequest<DescribeAclsResponse>, IEquatable<DescribeAclsRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},{this.AclToString()}}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(ResourceType)
                  .Write(ResourceName)
                  .Write(Principal)
                  .Write(Host)
                  .Write(Operation)
                  .Write(PermissionType);
        }

        public DescribeAclsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DescribeAclsResponse.FromBytes(context, bytes);

        public DescribeAclsRequest(byte resourceType, string resourceName, string principal, string host, byte operation, byte permissionType) 
            : base(ApiKey.DescribeAcls)
        {
            ResourceType = resourceType;
            ResourceName = resourceName;
            Principal = principal;
            Host = host;
            Operation = operation;
            PermissionType = permissionType;
        }

        /// <inheritdoc cref="IResource.ResourceType"/>
        public byte ResourceType { get; }

        /// <inheritdoc cref="IResource.ResourceName"/>
        public string ResourceName { get; }

        /// <inheritdoc cref="IAcl.Principal"/>
        public string Principal { get; }

        /// <inheritdoc cref="IAcl.Host"/>
        public string Host { get; }

        /// <inheritdoc cref="IAcl.Operation"/>
        public byte Operation { get; }

        /// <inheritdoc cref="IAcl.PermissionType"/>
        public byte PermissionType { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeAclsRequest);
        }

        public bool Equals(DescribeAclsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ResourceType == other.ResourceType 
                && string.Equals(ResourceName, other.ResourceName)
                && string.Equals(Principal, other.Principal) 
                && string.Equals(Host, other.Host) 
                && Operation == other.Operation 
                && PermissionType == other.PermissionType;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Principal?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Host?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Operation.GetHashCode();
                hashCode = (hashCode * 397) ^ PermissionType.GetHashCode();
                hashCode = (hashCode * 397) ^ ResourceType.GetHashCode();
                hashCode = (hashCode * 397) ^ (ResourceName?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion
    }
}