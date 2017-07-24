using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class EndpointTests
    {
        [Test]
        public void ThrowsExceptionIfIpNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => {
                    var x = new Endpoint(null);
                });
        }

        [Test]
        public async Task EnsureEndpointCanBeResolved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            Assert.AreEqual(expected, endpoint.Ip.Address);
            Assert.AreEqual(8888, endpoint.Ip.Port);
        }

        [Test]
        public async Task EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);

            Assert.False(ReferenceEquals(endpoint1, endpoint2), "Should not be the same reference.");
            Assert.AreEqual(endpoint1, endpoint2);
        }

        [Test]
        public async Task EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:1"), TestConfig.Log);

            Assert.AreNotEqual(endpoint1, endpoint2);
        }
    }
}