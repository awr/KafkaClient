using System;

namespace KafkaClient.Protocol
{
    public class Acl : IAcl, IEquatable<Acl>
    {
        public override string ToString() => $"{{principal:{Principal},host:{Host},operation:{Operation},permission_type:{PermissionType}}}";

        public Acl(string principal, string host, byte operation, byte permissionType)
        {
            Principal = principal;
            Host = host;
            Operation = operation;
            PermissionType = permissionType;
        }

        /// <inheritdoc cref="Principal"/>
        public string Principal { get; }

        /// <inheritdoc cref="Host"/>
        public string Host { get; }

        /// <inheritdoc cref="Operation"/>
        public byte Operation { get; }

        /// <inheritdoc cref="PermissionType"/>
        public byte PermissionType { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as Acl);
        }

        public bool Equals(Acl other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Principal, other.Principal) 
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
                return hashCode;
            }
        }
    }
}