using System;

namespace KafkaClient.Protocol
{
    public class ConfigEntry : IEquatable<ConfigEntry>
    {
        public override string ToString() => $"{{config_name:{ConfigName},config_value:{ConfigValue}}}";

        public ConfigEntry(string configName, string configValue)
        {
            ConfigName = configName;
            ConfigValue = configValue;
        }

        public string ConfigName { get; }

        public string ConfigValue { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as ConfigEntry);
        }

        public bool Equals(ConfigEntry other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ConfigName == other.ConfigName
                && ConfigValue == other.ConfigValue;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = ConfigName?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (ConfigValue?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}