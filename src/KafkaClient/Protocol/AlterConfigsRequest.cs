using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// AlterConfigs Request => [resources] validate_only 
    /// </summary>
    /// <remarks>
    /// AlterConfigs Request => [resources] validate_only 
    ///   resources => resource_type resource_name [config_entries] 
    ///     resource_type => INT8
    ///     resource_name => STRING
    ///     config_entries => config_name config_value 
    ///       config_name => STRING
    ///       config_value => NULLABLE_STRING
    ///   validate_only => BOOLEAN
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_AlterConfigs
    /// </remarks>
    public class AlterConfigsRequest : Request, IRequest<AlterConfigsResponse>, IEquatable<AlterConfigsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},resources:[{Resources.ToStrings()}],validate_only:{ValidateOnly}}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Resources.Count);
            foreach (var resource in Resources) {
                writer.Write(resource.ResourceType)
                      .Write(resource.ResourceName)
                      .Write(resource.ConfigEntries.Count);
                foreach (var config in resource.ConfigEntries) {
                    writer.Write(config.ConfigName)
                          .Write(config.ConfigValue);
                }
            }
            writer.Write(ValidateOnly);
        }

        public AlterConfigsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => AlterConfigsResponse.FromBytes(context, bytes);

        public AlterConfigsRequest(IEnumerable<ConfigResource> resources, bool validateOnly) 
            : base(ApiKey.AlterConfigs)
        {
            Resources = resources.ToSafeImmutableList();
            ValidateOnly = validateOnly;
        }

        /// <summary>
        ///The config resources to be returned.
        /// </summary>
        public IImmutableList<ConfigResource> Resources { get; }

        public bool ValidateOnly { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as AlterConfigsRequest);
        }

        public bool Equals(AlterConfigsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ValidateOnly == other.ValidateOnly
                && Resources.HasEqualElementsInOrder(other.Resources);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = ValidateOnly.GetHashCode();
                hashCode = (hashCode * 397) ^ Resources.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class ConfigResource : IResource, IEquatable<ConfigResource>
        {
            public override string ToString() => $"{{resource_type:{ResourceType},resource_name:{ResourceName},config_entries:[{ConfigEntries.ToStrings()}]}}";

            public ConfigResource(byte resourceType, string resourceName, IEnumerable<ConfigEntry> configEntries = null)
            {
                ResourceType = resourceType;
                ResourceName = resourceName;
                ConfigEntries = configEntries.ToSafeImmutableList();
            }

            /// <summary>
            /// Id of the resource type to alter configuration of. Value 2 means topic, 4 means broker
            /// </summary>
            public byte ResourceType { get; }

            /// <summary>
            /// Name of the resource to alter configuration of.
            /// </summary>
            public string ResourceName { get; }

            /// <summary>
            /// Configuration entries to alter.
            /// </summary>
            public IImmutableList<ConfigEntry> ConfigEntries { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as ConfigResource);
            }

            public bool Equals(ConfigResource other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ResourceType == other.ResourceType 
                    && string.Equals(ResourceName, other.ResourceName)
                    && ConfigEntries.HasEqualElementsInOrder(other.ConfigEntries);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = ResourceType.GetHashCode();
                    hashCode = (hashCode * 397) ^ (ResourceName?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ ConfigEntries.Count.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}