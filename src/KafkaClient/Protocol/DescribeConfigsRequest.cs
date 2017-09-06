using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeConfigs Request => [resources] 
    /// </summary>
    /// <remarks>
    /// DescribeConfigs Request => [resources] 
    ///   resources => resource_type resource_name [config_names] 
    ///     resource_type => INT8
    ///     resource_name => STRING
    ///     config_names => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeConfigs
    /// </remarks>
    public class DescribeConfigsRequest : Request, IRequest<DescribeConfigsResponse>, IEquatable<DescribeConfigsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},resources:[{Resources.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Resources.Count);
            foreach (var resource in Resources) {
                writer.Write(resource.ResourceType)
                      .Write(resource.ResourceName)
                      .Write(resource.ConfigNames, true);
            }
        }

        public DescribeConfigsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DescribeConfigsResponse.FromBytes(context, bytes);

        public DescribeConfigsRequest(IEnumerable<ConfigResource> resources = null) 
            : base(ApiKey.DescribeConfigs)
        {
            Resources = resources.ToSafeImmutableList();
        }

        /// <summary>
        ///The config resources to be returned.
        /// </summary>
        public IImmutableList<ConfigResource> Resources { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeConfigsRequest);
        }

        public bool Equals(DescribeConfigsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Resources.HasEqualElementsInOrder(other.Resources);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Resources.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class ConfigResource : IResource, IEquatable<ConfigResource>
        {
            public override string ToString() => $"{{resource_type:{ResourceType},resource_name:{ResourceName},config_names:[{ConfigNames.ToStrings()}]}}";

            public ConfigResource(byte resourceType, string resourceName, IEnumerable<string> configNames = null)
            {
                ResourceType = resourceType;
                ResourceName = resourceName;
                ConfigNames = configNames.ToSafeImmutableList();
            }

            /// <summary>
            /// Id of the resource type to fetch configuration of. Value 2 means topic, 4 means broker.
            /// </summary>
            public byte ResourceType { get; }

            /// <summary>
            /// Name of the resource to fetch configuration of.
            /// </summary>
            public string ResourceName { get; }

            /// <summary>
            /// Configuration names requested. Null for all configurations.
            /// </summary>
            public IImmutableList<string> ConfigNames { get; }

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
                    && ConfigNames.HasEqualElementsInOrder(other.ConfigNames);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = ResourceType.GetHashCode();
                    hashCode = (hashCode * 397) ^ (ResourceName?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ ConfigNames.Count.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}