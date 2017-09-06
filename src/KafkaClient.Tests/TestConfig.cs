using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Tests
{
    public static class TestConfig
    {
        public static string TopicName([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Topic-{name}";
        }

        public static string GroupId([CallerMemberName] string name = null)
        {
            return $"{Environment.MachineName}-Group-{name}-{Guid.NewGuid():N}";
        }

        // turned down to reduce log noise -- turn up Level if necessary
        public static readonly ILog Log = new ConsoleLog();

        public static Endpoint ServerEndpoint()
        {
            return new Endpoint(new IPEndPoint(IPAddress.Loopback, ServerPort()), "tcp://localhost");
        }

        public static int ServerPort()
        {
            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)) {
                socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                return ((IPEndPoint) socket.LocalEndPoint).Port;
            }
        }

        public static Uri IntegrationUri { get; } = new Uri("tcp://kafka:9092");

        public static KafkaOptions Options { get; } = new KafkaOptions(
            IntegrationUri,
            new ConnectionConfiguration(ConnectionConfiguration.Defaults.ConnectionRetry(TimeSpan.FromSeconds(10)), requestTimeout: TimeSpan.FromSeconds(10)),
            new RouterConfiguration(Retry.AtMost(2)),
            producerConfiguration: new ProducerConfiguration(stopTimeout: TimeSpan.FromSeconds(1)),
            consumerConfiguration: new ConsumerConfiguration(TimeSpan.FromMilliseconds(50), maxPartitionFetchBytes: 4096 * 8, heartbeatTimeout: TimeSpan.FromSeconds(6)),
            log: Log);

        public static KafkaOptions IntegrationOptions { get; } = new KafkaOptions(
            IntegrationUri,
            new ConnectionConfiguration(ConnectionConfiguration.Defaults.ConnectionRetry(TimeSpan.FromSeconds(10)), requestTimeout: TimeSpan.FromSeconds(10)),
            producerConfiguration: new ProducerConfiguration(stopTimeout: TimeSpan.FromSeconds(1)),
            consumerConfiguration: new ConsumerConfiguration(heartbeatTimeout: TimeSpan.FromSeconds(6)),
            log: Log);
    }
}