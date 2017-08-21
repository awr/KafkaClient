using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeConfigs Response => throttle_time_ms [resources] 
    /// </summary>
    /// <remarks>
    /// DescribeConfigs Response => throttle_time_ms [resources] 
    ///   throttle_time_ms => INT32
    ///   resources => error_code error_message resource_type resource_name [config_entries] 
    ///     error_code => INT16
    ///     error_message => NULLABLE_STRING
    ///     resource_type => INT8
    ///     resource_name => STRING
    ///     config_entries => config_name config_value read_only is_default is_sensitive 
    ///       config_name => STRING
    ///       config_value => NULLABLE_STRING
    ///       read_only => BOOLEAN
    ///       is_default => BOOLEAN
    ///       is_sensitive => BOOLEAN
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeConfigs
    /// </remarks>
    public class DescribeConfigsResponse : ThrottledResponse, IResponse, IEquatable<DescribeConfigsResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{ThrottleTime},resources:{Resources.ToStrings()}}}";

        public static DescribeConfigsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();

                var resourceCount = reader.ReadInt32();
                var resources = new ConfigResource[resourceCount];
                for (var r = 0; r < resourceCount; r++ ) {
                    var errorCode = reader.ReadErrorCode();
                    var errorMessage = reader.ReadString();
                    var resourceType = reader.ReadByte();
                    var resourceName = reader.ReadString();
                    var configCount = reader.ReadInt32();
                    var configs = new ConfigEntry[configCount];
                    for (var c = 0; c < configCount; c++) {
                        var configName = reader.ReadString();
                        var configValue = reader.ReadString();
                        var readOnly = reader.ReadBoolean();
                        var isDefault = reader.ReadBoolean();
                        var isSensitive = reader.ReadBoolean();

                        configs[c] = new ConfigEntry(configName, configValue, readOnly, isDefault, isSensitive);
                    }
                    resources[r] = new ConfigResource(errorCode, errorMessage, resourceType, resourceName, configs);
                }
                return new DescribeConfigsResponse(throttleTime.Value, resources);
            }
        }

        public DescribeConfigsResponse(TimeSpan throttleTime, IEnumerable<ConfigResource> resources = null)
            : base(throttleTime)
        {
            Resources = resources.ToSafeImmutableList();
            Errors = Resources.Select(r => r.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The resources and their associated ACLs.
        /// </summary>
        public IImmutableList<ConfigResource> Resources { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeConfigsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DescribeConfigsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Resources.HasEqualElementsInOrder(other.Resources);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Resources.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class ConfigResource : Protocol.ConfigResource, IEquatable<ConfigResource>
        {
            public override string ToString() => $"{{error_code:{Error},error_message:{ErrorMessage},resource_type:{ResourceType},resource_name:{ResourceName},config_entries:[{ConfigEntries.ToStrings()}]}}";

            public ConfigResource(ErrorCode errorCode, string errorMessage, byte resourceType, string resourceName, IEnumerable<ConfigEntry> configEntries = null)
                : base(errorCode, errorMessage, resourceType, resourceName)
            {
                ConfigEntries = configEntries.ToSafeImmutableList();
            }

            public IImmutableList<ConfigEntry> ConfigEntries { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as ConfigResource);
            }

            public bool Equals(ConfigResource other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals((Protocol.ConfigResource)other)
                    && ConfigEntries.HasEqualElementsInOrder(other.ConfigEntries);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ ConfigEntries.Count.GetHashCode();
                    return hashCode;
                }
            }
        }

        public class ConfigEntry : Protocol.ConfigEntry, IEquatable<ConfigEntry>
        {
            public override string ToString() => $"{{config_name:{ConfigName},config_value:{ConfigValue},read_only:{ReadOnly},is_default:{IsDefault},is_sensitive:{IsSensitive}}}";

            public ConfigEntry(string configName, string configValue, bool readOnly, bool isDefault, bool isSensitive)
                : base(configName, configValue)
            {
                ReadOnly = readOnly;
                IsDefault = isDefault;
                IsSensitive = isSensitive;
            }

            public bool ReadOnly { get; }

            public bool IsDefault { get; }

            public bool IsSensitive { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as ConfigEntry);
            }

            public bool Equals(ConfigEntry other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals((Protocol.ConfigEntry)other)
                    && ReadOnly == other.ReadOnly 
                    && IsDefault == other.IsDefault 
                    && IsSensitive == other.IsSensitive;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ ReadOnly.GetHashCode();
                    hashCode = (hashCode * 397) ^ IsDefault.GetHashCode();
                    hashCode = (hashCode * 397) ^ IsSensitive.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}