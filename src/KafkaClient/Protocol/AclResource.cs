using System;

namespace KafkaClient.Protocol
{
    public class AclResource : Acl, IAclResource, IEquatable<AclResource>
    {
        public override string ToString() => $"{{{this.AclToString()}}}";

        public AclResource(byte resourceType, string resourceName, string principal, string host, byte operation, byte permissionType) 
            : base(principal, host, operation, permissionType)
        {
            ResourceType = resourceType;
            ResourceName = resourceName;
        }

        /// <inheritdoc cref="ResourceType"/>
        public byte ResourceType { get; }

        /// <inheritdoc cref="ResourceName"/>
        public string ResourceName { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as AclResource);
        }

        public bool Equals(AclResource other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals((Acl)other) 
                && ResourceType == other.ResourceType 
                && string.Equals(ResourceName, other.ResourceName);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ ResourceType.GetHashCode();
                hashCode = (hashCode * 397) ^ (ResourceName?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}