using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public class VersionSupport : IVersionSupport
    {
        private static VersionSupport _kafka8;
        /// <summary>
        /// 0.8.2 &lt;= Version &lt; 0.9
        /// </summary>
        public static VersionSupport Kafka8 => _kafka8 ?? (_kafka8 = new VersionSupport(
            new Dictionary<ApiKey, short> {
                {ApiKey.Produce, 0},
                {ApiKey.Fetch, 0},
                {ApiKey.Offsets, 0},
                {ApiKey.Metadata, 0},
                {ApiKey.OffsetCommit, 1},
                {ApiKey.OffsetFetch, 1},
                {ApiKey.GroupCoordinator, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka9;
        /// <summary>
        /// 0.9 &lt;= Version &lt; 0.10
        /// </summary>
        public static VersionSupport Kafka9 => _kafka9 ?? (_kafka9 = new VersionSupport(
            new Dictionary<ApiKey, short> {
                {ApiKey.Produce, 1},
                {ApiKey.Fetch, 1},
                {ApiKey.Offsets, 0},
                {ApiKey.Metadata, 0},
                {ApiKey.OffsetCommit, 2},
                {ApiKey.OffsetFetch, 1},
                {ApiKey.GroupCoordinator, 0},
                {ApiKey.JoinGroup, 0},
                {ApiKey.Heartbeat, 0},
                {ApiKey.LeaveGroup, 0},
                {ApiKey.SyncGroup, 0},
                {ApiKey.DescribeGroups, 0},
                {ApiKey.ListGroups, 0},
                {ApiKey.SaslHandshake, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka10;
        /// <summary>
        /// 0.10 &lt;= Version 
        /// </summary>
        public static VersionSupport Kafka10 => _kafka10 ?? (_kafka10 = new VersionSupport(
            new Dictionary<ApiKey, short> {
                {ApiKey.Produce, 2},
                {ApiKey.Fetch, 2},
                {ApiKey.Offsets, 0},
                {ApiKey.Metadata, 1},
                {ApiKey.OffsetCommit, 2},
                {ApiKey.OffsetFetch, 1},
                {ApiKey.GroupCoordinator, 0},
                {ApiKey.JoinGroup, 0},
                {ApiKey.Heartbeat, 0},
                {ApiKey.LeaveGroup, 0},
                {ApiKey.SyncGroup, 0},
                {ApiKey.DescribeGroups, 0},
                {ApiKey.ListGroups, 0},
                {ApiKey.SaslHandshake, 0},
                {ApiKey.ApiVersions, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka10_1;
        /// <summary>
        /// 0.10 &lt;= Version 
        /// </summary>
        public static VersionSupport Kafka10_1 => _kafka10_1 ?? (_kafka10_1 = new VersionSupport(
            new Dictionary<ApiKey, short> {
                {ApiKey.Produce, 2},
                {ApiKey.Fetch, 3},
                {ApiKey.Offsets, 1},
                {ApiKey.Metadata, 2},
                {ApiKey.OffsetCommit, 2},
                {ApiKey.OffsetFetch, 1},
                {ApiKey.GroupCoordinator, 0},
                {ApiKey.JoinGroup, 1},
                {ApiKey.Heartbeat, 0},
                {ApiKey.LeaveGroup, 0},
                {ApiKey.SyncGroup, 0},
                {ApiKey.DescribeGroups, 0},
                {ApiKey.ListGroups, 0},
                {ApiKey.SaslHandshake, 0},
                {ApiKey.ApiVersions, 0},
                {ApiKey.CreateTopics, 0},
                {ApiKey.DeleteTopics, 0}
            }.ToImmutableDictionary()));

        private readonly IImmutableDictionary<ApiKey, short> _versionSupport;

        public VersionSupport(IImmutableDictionary<ApiKey, short> versionSupport)
        {
            _versionSupport = versionSupport;
        }

        public short? GetVersion(ApiKey apiKey)
        {
            return _versionSupport.TryGetValue(apiKey, out short version) ? version : (short?)null;
        }
    }
}