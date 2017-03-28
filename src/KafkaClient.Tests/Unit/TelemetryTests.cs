using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Telemetry;
using NUnit.Framework;
using KafkaClient.Testing;
using KafkaClient.Common;
using System.IO;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class TelemetryTests
    {
        #region ConnectionStatistics

        [Test]
        public async Task TracksConnectionAttemptsCorrectly([Values(1, 10)] int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
                var tasks = new List<Task>();
                for (var i = 0; i < total; i++) {
                    tasks.Add(conn.SendAsync(new FetchRequest(), CancellationToken.None));
                }
                var requestTasks = Task.WhenAll(tasks);
                await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));
            }

            Assert.That(telemetry.TcpConnections.Count, Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Connects), Is.EqualTo(0));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Attempts), Is.EqualTo(total));
        }

        [Test]
        public async Task TracksConnectionSuccessCorrectly([Values(1, 10)] int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var endpoint = TestConfig.ServerEndpoint();
            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
                var tasks = new List<Task>();
                for (var i = 0; i < total; i++) {
                    tasks.Add(conn.SendAsync(new FetchRequest(), CancellationToken.None));
                }
                var requestTasks = Task.WhenAll(tasks);
                await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));
            }

            Assert.That(telemetry.TcpConnections.Count, Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Connects), Is.EqualTo(1));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Attempts), Is.EqualTo(total));
        }

        private static byte[] CreateCorrelationMessage(int id)
        {
            var buffer = new byte[8];
            var stream = new MemoryStream(buffer);
            stream.Write(Request.CorrelationSize.ToBytes(), 0, 4);
            stream.Write(id.ToBytes(), 0, 4);
            return buffer;
        }

        [Test]
        public async Task TracksDisconnectsCorrectly([Values(1, 3)] int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var log = new MemoryLog();
            var serverConnected = 0;

            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log) {
                OnConnected = () => Interlocked.Increment(ref serverConnected)
            })
            using (new Connection(endpoint, config, log: log)) {
                for (var connectionAttempt = 1; connectionAttempt <= total; connectionAttempt++) {
                    var currentAttempt = connectionAttempt;
                    await AssertAsync.ThatEventually(() => serverConnected == currentAttempt, () => $"server {serverConnected}, attempt {currentAttempt}");
                    await server.SendDataAsync(new ArraySegment<byte>(CreateCorrelationMessage(connectionAttempt)));
                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Sent CONNECTION attempt {currentAttempt}"));

                    await AssertAsync.ThatEventually(() => log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Received 4 bytes (id ")) == currentAttempt, () => $"attempt {currentAttempt}\n" + log.ToString(LogLevel.Info));

                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Dropping CONNECTION attempt {currentAttempt}"));
                    server.DropConnection();

                    Assert.That(log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Disposing transport to")), Is.AtLeast(currentAttempt));
                }
            }

            //Assert.That(telemetry.TcpConnections.Count, Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Attempts), Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Connects), Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Disconnects), Is.EqualTo(total));
        }


        // disconnects
        // multiple connects
        // connects over multiple time slices

        #endregion

        #region TcpStatistics

        #endregion


        #region ApiStatistics

        [Test]
        public async Task TracksRequestFailuresCorrectly([Values(1, 10)] int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
                var tasks = new List<Task>();
                for (var i = 0; i < total; i++) {
                    tasks.Add(conn.SendAsync(new FetchRequest(), CancellationToken.None));
                }
                var requestTasks = Task.WhenAll(tasks);
                await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));
            }

            Assert.That(telemetry.Requests.Count, Is.EqualTo(total));
            Assert.That(telemetry.Requests.Sum(t => t.Successes.GetOrDefault(ApiKey.Fetch)), Is.EqualTo(0));
            Assert.That(telemetry.Requests.Sum(t => t.Attempts.GetOrDefault(ApiKey.Fetch)), Is.EqualTo(total));
            Assert.That(telemetry.Requests.Sum(t => t.Failures.GetOrDefault(ApiKey.Fetch)), Is.EqualTo(total));
            Assert.That(telemetry.Requests.Sum(t => t.Successes.Sum(p => p.Value)), Is.EqualTo(0));
            Assert.That(telemetry.Requests.Sum(t => t.Attempts.Sum(p => p.Value)), Is.EqualTo(total));
            Assert.That(telemetry.Requests.Sum(t => t.Failures.Sum(p => p.Value)), Is.EqualTo(total));
        }

        // success
        // multiple time slices ...

        #endregion

    }
}