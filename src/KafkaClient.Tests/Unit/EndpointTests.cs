using System;
using System.Net;
using System.Threading.Tasks;
using KafkaClient.Connections;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    public class EndpointTests
    {
        [Fact]
        public void ThrowsExceptionIfIpNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => {
                    var x = new Endpoint(null);
                });
        }

        [Fact]
        public async Task EnsureEndpointCanBeResolved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            Assert.Equal(expected, endpoint.Ip.Address);
            Assert.Equal(8888, endpoint.Ip.Port);
        }

        [Fact]
        public async Task EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);

            Assert.False(ReferenceEquals(endpoint1, endpoint2), "Should not be the same reference.");
            Assert.Equal(endpoint1, endpoint2);
        }

        [Fact]
        public async Task EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:8888"), TestConfig.Log);
            var endpoint2 = await Endpoint.ResolveAsync(new Uri("tcp://localhost:1"), TestConfig.Log);

            Assert.NotEqual(endpoint1, endpoint2);
        }
    }
}