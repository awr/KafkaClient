using System;

namespace KafkaClient.Protocol
{
    public class ConfigResource : IResource, IEquatable<ConfigResource>
    {
        public override string ToString() => $"{{error_code:{Error},error_message:{ErrorMessage},resource_type:{ResourceType},resource_name:{ResourceName}}}";

        public ConfigResource(ErrorCode errorCode, string errorMessage, byte resourceType, string resourceName)
        {
            Error = errorCode;
            ErrorMessage = errorMessage;
            ResourceType = resourceType;
            ResourceName = resourceName;
        }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode Error { get; }

        /// <summary>
        /// The error message.
        /// </summary>
        public string ErrorMessage { get; }

        /// <inheritdoc cref="IResource.ResourceType"/>
        public byte ResourceType { get; }

        /// <inheritdoc cref="IResource.ResourceName"/>
        public string ResourceName { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as ConfigResource);
        }

        public bool Equals(ConfigResource other)
        {
            if (Object.ReferenceEquals(null, other)) return false;
            if (Object.ReferenceEquals(this, other)) return true;
            return Error == other.Error 
                && string.Equals((string) ErrorMessage, (string) other.ErrorMessage)
                && ResourceType == other.ResourceType 
                && string.Equals((string) ResourceName, (string) other.ResourceName);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Error.GetHashCode();
                hashCode = (hashCode * 397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ ResourceType.GetHashCode();
                hashCode = (hashCode * 397) ^ (ResourceName?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}