﻿using System;
using System.Net;
using System.Runtime.Serialization;

namespace KafkaClient.Connection
{
    [Serializable]
    public class KafkaEndpoint
    {
        public Uri ServerUri { get; }
        public IPEndPoint Endpoint { get; }

        protected bool Equals(KafkaEndpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in the desc order
            return Endpoint?.Address.GetHashCode() + Endpoint?.Port ?? 0;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as KafkaEndpoint);
        }

        public override string ToString() => ServerUri.ToString();

        public KafkaEndpoint(Uri serverUri, IPEndPoint endpoint)
        {
            ServerUri = serverUri;
            Endpoint = endpoint;
        }

        public KafkaEndpoint(SerializationInfo info, StreamingContext context)
        {
            ServerUri = info.GetValue<Uri>("ServerUri");
            Endpoint = info.GetValue<IPEndPoint>("Endpoint");
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ServerUri", ServerUri);
            info.AddValue("Endpoint", Endpoint);
        }
    }
}