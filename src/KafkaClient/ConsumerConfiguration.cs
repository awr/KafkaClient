using System;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class ConsumerConfiguration : IConsumerConfiguration
    {
        private static readonly Lazy<ConsumerConfiguration> LazyDefault = new Lazy<ConsumerConfiguration>(() => new ConsumerConfiguration());
        public static IConsumerConfiguration Default => LazyDefault.Value;

        public ConsumerConfiguration(
            TimeSpan? maxServerWait = null, 
            int? minFetchBytes = null, 
            int? maxFetchBytes = null, 
            int? maxPartitionFetchBytes = null,
            int? fetchByteMultiplier = null,
            TimeSpan? heartbeatTimeout = null,
            TimeSpan? rebalanceTimeout = null,
            IRetry coordinationRetry = null,
            int batchSize = Defaults.BatchSize)
        {
            MaxFetchServerWait = maxServerWait;
            MinFetchBytes = minFetchBytes;
            MaxFetchBytes = maxFetchBytes;
            FetchByteMultiplier = fetchByteMultiplier.GetValueOrDefault(Defaults.FetchByteMultiplier);
            MaxPartitionFetchBytes = maxPartitionFetchBytes;
            GroupHeartbeat = heartbeatTimeout ?? TimeSpan.FromSeconds(Defaults.HeartbeatSeconds);
            GroupRebalanceTimeout = rebalanceTimeout ?? heartbeatTimeout ?? TimeSpan.FromSeconds(Defaults.RebalanceTimeoutSeconds);
            GroupCoordinationRetry = coordinationRetry ?? Defaults.CoordinationRetry(GroupRebalanceTimeout);
            BatchSize = Math.Max(1, batchSize);
        }

        /// <inheritdoc/>
        public int? MaxFetchBytes { get; }
        /// <inheritdoc/>
        public int? MaxPartitionFetchBytes { get; }
        /// <inheritdoc/>
        public int? MinFetchBytes { get; }
        /// <inheritdoc/>
        public int FetchByteMultiplier { get; }
        /// <inheritdoc/>
        public TimeSpan? MaxFetchServerWait { get; }

        /// <inheritdoc/>
        public TimeSpan GroupRebalanceTimeout { get; }
        /// <inheritdoc/>
        public TimeSpan GroupHeartbeat { get; }
        /// <inheritdoc/>
        public IRetry GroupCoordinationRetry { get; }
        /// <inheritdoc/>
        public int BatchSize { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default <see cref="GroupHeartbeat"/> seconds
            /// </summary>
            public const int HeartbeatSeconds = RebalanceTimeoutSeconds;

            /// <summary>
            /// The default <see cref="GroupRebalanceTimeout"/> seconds
            /// </summary>
            public const int RebalanceTimeoutSeconds = ConnectionConfiguration.Defaults.RequestTimeoutSeconds / 2;

            /// <summary>
            /// The default <see cref="GroupCoordinationRetry"/> backoff delay
            /// </summary>
            public const int GroupCoordinationRetryMilliseconds = 100;

            /// <summary>
            /// The default <see cref="ConsumerConfiguration.BatchSize"/>
            /// </summary>
            public const int BatchSize = 100;

            /// <summary>
            /// The default <see cref="ConsumerConfiguration.FetchByteMultiplier"/>
            /// </summary>
            public const int FetchByteMultiplier = 2;

            public static IRetry CoordinationRetry(TimeSpan? timeout = null)
            {
                return Retry.Until(
                    timeout ?? TimeSpan.FromSeconds(HeartbeatSeconds),
                    TimeSpan.FromMilliseconds(GroupCoordinationRetryMilliseconds));
            }
        }
    }
}