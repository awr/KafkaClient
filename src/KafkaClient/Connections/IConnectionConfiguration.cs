﻿using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Telemetry;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Configuration for the tcp connection.
    /// </summary>
    public interface IConnectionConfiguration : IRaiseEvents
    {
        /// <summary>
        /// Retry details for (re)establishing the connection.
        /// </summary>
        IRetry ConnectionRetry { get; }

        /// <summary>
        /// Support for different protocol versions for Kakfa requests and responses.
        /// </summary>
        IVersionSupport VersionSupport { get; }

        /// <summary>
        /// The maximum time to wait for requests.
        /// </summary>
        TimeSpan RequestTimeout { get; }

        /// <summary>
        /// The buffer size to use for the socket, when receiving bytes
        /// </summary>
        int ReadBufferSize { get; }

        /// <summary>
        /// The buffer size to use for the socket, when sending bytes
        /// </summary>
        int WriteBufferSize { get; }

        /// <summary>
        /// TCP keepalive option
        /// </summary>
        bool IsTcpKeepalive { get; }

        /// <summary>
        /// Custom Encoding support for different protocol types
        /// </summary>
        IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        /// <summary>
        /// Configuration for SSL encrypted communication
        /// </summary>
        ISslConfiguration SslConfiguration { get; }
    }
}