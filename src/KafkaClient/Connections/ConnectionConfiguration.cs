using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Telemetry;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Configuration for the tcp connection.
    /// </summary>
    public class ConnectionConfiguration : IConnectionConfiguration
    {
        private static readonly Lazy<ConnectionConfiguration> LazyDefault = new Lazy<ConnectionConfiguration>();
        public static IConnectionConfiguration Default => LazyDefault.Value;

        /// <summary>
        /// Configuration for the tcp connection.
        /// </summary>
        /// <param name="connectionRetry">Retry details for (re)establishing the connection.</param>
        /// <param name="versionSupport">Support for different protocol versions for Kakfa requests and responses.</param>
        /// <param name="requestTimeout">The maximum time to wait for requests.</param>
        /// <param name="readBufferSize">The buffer size to use for the socket, when receiving bytes.</param>
        /// <param name="writeBufferSize">The buffer size to use for the socket, when sending bytes.</param>
        /// <param name="isTcpKeepalive">TCP keepalive option.</param>
        /// <param name="encoders">Custom Encoding support for different protocol types</param>
        /// <param name="sslConfiguration">Configuration for SSL encrypted communication</param>
        /// <param name="onDisconnected">Triggered when the tcp socket is disconnected.</param>
        /// <param name="onConnecting">Triggered when the tcp socket is connecting.</param>
        /// <param name="onConnected">Triggered after the tcp socket is successfully connected.</param>
        /// <param name="onWriting">Triggered when writing to the tcp stream.</param>
        /// <param name="onWritingBytes">Triggered when writing a chunk of bytes to the tcp stream.</param>
        /// <param name="onWroteBytes">Triggered after successfully writing a chunk of bytes to the tcp stream.</param>
        /// <param name="onWritten">Triggered after having successfully written to the tcp stream.</param>
        /// <param name="onWriteFailed">Triggered after failing to write to the tcp stream.</param>
        /// <param name="onReading">Triggered when starting to read a message's bytes from the tcp stream.</param>
        /// <param name="onReadingBytes">Triggered when reading a chunk of bytes from the tcp stream.</param>
        /// <param name="onReadBytes">Triggered after successfully reading a chunk of bytes from the tcp stream.</param>
        /// <param name="onRead">Triggered after having successfully read a message's bytes from the tcp stream.</param>
        /// <param name="onReadFailed">Triggered after failing to read from the tcp stream.</param>
        /// <param name="onProduceRequestMessages">Triggered when encoding ProduceRequest messages.</param>
        public ConnectionConfiguration(
            IRetry connectionRetry = null, 
            IVersionSupport versionSupport = null,
            TimeSpan? requestTimeout = null,
            int? readBufferSize = null, 
            int? writeBufferSize = null,
            bool? isTcpKeepalive = null,
            IEnumerable<IMembershipEncoder> encoders = null,
            ISslConfiguration sslConfiguration = null,
            ConnectError onDisconnected = null, 
            Connecting onConnecting = null, 
            Connecting onConnected = null, 
            Writing onWriting = null, 
            StartingBytes onWritingBytes = null, 
            FinishedBytes onWroteBytes = null, 
            WriteSuccess onWritten = null, 
            WriteError onWriteFailed = null, 
            Reading onReading = null, 
            StartingBytes onReadingBytes = null, 
            FinishedBytes onReadBytes = null, 
            ReadSuccess onRead = null, 
            ReadError onReadFailed = null,
            ProduceRequestMessages onProduceRequestMessages = null
            )
        {
            ConnectionRetry = connectionRetry ?? Defaults.ConnectionRetry();
            VersionSupport = versionSupport ?? Connections.VersionSupport.Kafka10;
            RequestTimeout = requestTimeout ?? TimeSpan.FromSeconds(Defaults.RequestTimeoutSeconds);
            ReadBufferSize = readBufferSize.GetValueOrDefault(Defaults.BufferSize);
            WriteBufferSize = writeBufferSize.GetValueOrDefault(Defaults.BufferSize);
            IsTcpKeepalive = isTcpKeepalive ?? Defaults.IsTcpKeepalive;
            Encoders = Defaults.Encoders(encoders);
            SslConfiguration = sslConfiguration;
            OnDisconnected = onDisconnected;
            OnConnecting = onConnecting;
            OnConnected = onConnected;
            OnWriting = onWriting;
            OnWritingBytes = onWritingBytes;
            OnWroteBytes = onWroteBytes;
            OnWritten = onWritten;
            OnWriteFailed = onWriteFailed;
            OnReading = onReading;
            OnReadingBytes = onReadingBytes;
            OnReadBytes = onReadBytes;
            OnRead = onRead;
            OnReadFailed = onReadFailed;
            OnProduceRequestMessages = onProduceRequestMessages;
        }

        /// <inheritdoc />
        public IRetry ConnectionRetry { get; }

        /// <inheritdoc />
        public IVersionSupport VersionSupport { get; }

        /// <inheritdoc />
        public TimeSpan RequestTimeout { get; }

        /// <inheritdoc />
        public int ReadBufferSize { get; }

        /// <inheritdoc />
        public int WriteBufferSize { get; }

        /// <inheritdoc />
        public bool IsTcpKeepalive { get; }

        /// <inheritdoc />
        public IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        /// <inheritdoc />
        public ISslConfiguration SslConfiguration { get; }

        /// <inheritdoc />
        public ConnectError OnDisconnected { get; }

        /// <inheritdoc />
        public Connecting OnConnecting { get; }

        /// <inheritdoc />
        public Connecting OnConnected { get; }

        /// <inheritdoc />
        public Writing OnWriting { get; }

        /// <inheritdoc />
        public StartingBytes OnWritingBytes { get; }

        /// <inheritdoc />
        public FinishedBytes OnWroteBytes { get; }

        /// <inheritdoc />
        public WriteSuccess OnWritten { get; }

        /// <inheritdoc />
        public WriteError OnWriteFailed { get; }

        /// <inheritdoc />
        public Reading OnReading { get; }

        /// <inheritdoc />
        public StartingBytes OnReadingBytes { get; }

        /// <inheritdoc />
        public FinishedBytes OnReadBytes { get; }

        /// <inheritdoc />
        public ReadSuccess OnRead { get; }

        /// <inheritdoc />
        public ReadError OnReadFailed { get; }

        /// <inheritdoc />
        public ProduceRequestMessages OnProduceRequestMessages { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default <see cref="ConnectionConfiguration.RequestTimeout"/> seconds
            /// </summary>
            public const int RequestTimeoutSeconds = 60;

            /// <summary>
            /// The default <see cref="ConnectionConfiguration.ConnectionRetry"/> timeout
            /// </summary>
            public const int ConnectingTimeoutMinutes = 5;

            /// <summary>
            /// The default size for <see cref="ConnectionConfiguration.ReadBufferSize"/> and <see cref="ConnectionConfiguration.WriteBufferSize"/>
            /// </summary>
            public const int BufferSize = 8192;

            /// <summary>
            /// The default value for <see cref="ConnectionConfiguration.IsTcpKeepalive"/> and <see cref="ConnectionConfiguration.IsTcpKeepalive"/>
            /// </summary>
            public const bool IsTcpKeepalive = true;

            /// <summary>
            /// The default max retries for <see cref="ConnectionConfiguration.ConnectionRetry"/>
            /// </summary>
            public const int MaxConnectionAttempts = 6;

            /// <summary>
            /// The default <see cref="ConnectionConfiguration.ConnectionRetry"/> backoff delay
            /// </summary>
            public const int ConnectingDelayMilliseconds = 100;

            public static IRetry ConnectionRetry(TimeSpan? timeout = null)
            {
                return Retry.WithBackoff(
                    MaxConnectionAttempts, 
                    timeout ?? TimeSpan.FromMinutes(ConnectingTimeoutMinutes), 
                    TimeSpan.FromMilliseconds(ConnectingDelayMilliseconds));
            }

            public static IImmutableDictionary<string, IMembershipEncoder> Encoders(params IMembershipEncoder[] encoders)
            {
                return Encoders((IEnumerable<IMembershipEncoder>) encoders);
            }

            public static IImmutableDictionary<string, IMembershipEncoder> Encoders(IEnumerable<IMembershipEncoder> encoders)
            {
                var defaultEncoders = encoders != null
                    ? encoders.ToImmutableDictionary(e => e.ProtocolType)
                    : ImmutableDictionary<string, IMembershipEncoder>.Empty;
                if (!defaultEncoders.ContainsKey(ConsumerEncoder.Protocol)) {
                    var consumerEncoder = new ConsumerEncoder(SimpleAssignor.Assignors);
                    defaultEncoders = defaultEncoders.Add(consumerEncoder.ProtocolType, consumerEncoder);
                }
                return defaultEncoders;
            }
        }
    }
}