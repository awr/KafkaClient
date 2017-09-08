using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    /// <summary>
    /// An exception cause by a failure in the connection to Kafka
    /// </summary>
    public class ConnectionException : KafkaException
    {
        public ConnectionException(Endpoint endpoint)
            : base($"Failure on connection to {endpoint}")
        {
            _endpoints = ImmutableList<Endpoint>.Empty.Add(endpoint);
        }

        public ConnectionException(IList<Endpoint> endpoints, Exception innerException)
            : base($"Failure on connection to {endpoints.ToStrings()}", innerException)
        {
            _endpoints = endpoints.ToSafeImmutableList();
        }

        public ConnectionException(string message)
            : base(message)
        {
            _endpoints = ImmutableList<Endpoint>.Empty;
        }

        // ReSharper disable once NotAccessedField.Local -- for debugging
        private readonly IImmutableList<Endpoint> _endpoints;
    }
}