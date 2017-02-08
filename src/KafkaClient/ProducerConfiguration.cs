using System;

namespace KafkaClient
{
    public class ProducerConfiguration : IProducerConfiguration
    {
        private static readonly Lazy<ProducerConfiguration> LazyDefault = new Lazy<ProducerConfiguration>();
        public static IProducerConfiguration Default => LazyDefault.Value;

        public ProducerConfiguration(
            int requestParallelization = Defaults.RequestParallelization, 
            int batchSize = Defaults.BatchSize, 
            TimeSpan? batchMaxDelay = null,
            TimeSpan? stopTimeout = null,
            IPartitionSelector partitionSelector = null,
            ISendMessageConfiguration sendDefaults = null)
        {
            RequestParallelization = requestParallelization;
            BatchSize = Math.Max(1, batchSize);
            BatchMaxDelay = batchMaxDelay ?? TimeSpan.FromMilliseconds(Defaults.BatchMaxDelayMilliseconds);
            StopTimeout = stopTimeout ?? TimeSpan.FromSeconds(Defaults.DefaultStopTimeoutSeconds);
            PartitionSelector = partitionSelector ?? new PartitionSelector();
            SendDefaults = sendDefaults ?? new SendMessageConfiguration();
        }

        /// <inheritdoc />
        public int RequestParallelization { get; }

        /// <inheritdoc />
        public int BatchSize { get; }

        /// <inheritdoc />
        public TimeSpan BatchMaxDelay { get; }

        /// <inheritdoc />
        public TimeSpan StopTimeout { get; }

        /// <inheritdoc />
        public ISendMessageConfiguration SendDefaults { get; }

        /// <inheritdoc />
        public IPartitionSelector PartitionSelector { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default value for <see cref="ProducerConfiguration.RequestParallelization"/>
            /// </summary>
            public const int RequestParallelization = 20;

            /// <summary>
            /// The default value for <see cref="BatchMaxDelay"/>
            /// </summary>
            public const int BatchMaxDelayMilliseconds = 100;

            /// <summary>
            /// The default value for <see cref="ProducerConfiguration.BatchSize"/>
            /// </summary>
            public const int BatchSize = 100;

            /// <summary>
            /// The default value for <see cref="StopTimeout"/>
            /// </summary>
            public const int DefaultStopTimeoutSeconds = 5;
        }
    }
}