using System;
using KafkaClient.Common;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    public class SocketTransportTests : TransportTests<SocketTransport>
    {
        [Test]
        public void CreatingWithNullEndpointThrowsException()
        {
            var config = new ConnectionConfiguration(sslConfiguration: new SslConfiguration());
            Assert.Throws<ArgumentNullException>(() => new ReconnectingSocket(null, config, TestConfig.Log, false));
        }

        [Test]
        public void CreatingWithSslConfigurationThrowsException()
        {
            var config = new ConnectionConfiguration(sslConfiguration: new SslConfiguration());
            Assert.Throws<ArgumentOutOfRangeException>(
                () => {
                    using (new SocketTransport(TestConfig.ServerEndpoint(), config, TestConfig.Log)) { }
                });
        }

        protected override SocketTransport CreateTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            return new SocketTransport(endpoint, configuration, log);
        }
    }
}