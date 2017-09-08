using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Telemetry;
using KafkaClient.Testing;
using KafkaClient.Common;
using System.IO;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class TelemetryTests
    {
        #region ConnectionStatistics

        [TestCase(1)]
        [TestCase(10)]
        [Category("Flaky")]
        public async Task TracksConnectionAttemptsCorrectly(int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var endpoint = TestConfig.ServerEndpoint();
            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            var tasks = new List<Task>();
            for (var i = 0; i < total; i++) {
                tasks.Add(FetchAsync(endpoint, config));
            }
            var requestTasks = Task.WhenAll(tasks);
            await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));

            Assert.AreEqual(1, telemetry.TcpConnections.Count);
            Assert.AreEqual(0, telemetry.TcpConnections.Sum(t => t.Connects));
            Assert.AreEqual(total, telemetry.TcpConnections.Sum(t => t.Attempts));
        }

        private async Task FetchAsync(Endpoint endpoint, IConnectionConfiguration config)
        {
            using (var conn = new Connection(endpoint, config, TestConfig.Log)) {
                await conn.SendAsync(new FetchRequest(), CancellationToken.None);
            }
        }

        [TestCase(1)]
        [TestCase(10)]
        public async Task TracksConnectionSuccessCorrectly(int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var endpoint = TestConfig.ServerEndpoint();
            var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            using (new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                var tasks = new List<Task>();
                for (var i = 0; i < total; i++) {
                    tasks.Add(FetchAsync(endpoint, config));
                }
                var requestTasks = Task.WhenAll(tasks);
                await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));
            }

            Assert.AreEqual(1, telemetry.TcpConnections.Count);
            Assert.AreEqual(total, telemetry.TcpConnections.Sum(t => t.Connects));
            Assert.AreEqual(total, telemetry.TcpConnections.Sum(t => t.Attempts));
        }

        private static byte[] CreateCorrelationMessage(int id)
        {
            var buffer = new byte[8];
            var stream = new MemoryStream(buffer);
            stream.Write(Request.CorrelationSize.ToBytes(), 0, 4);
            stream.Write(id.ToBytes(), 0, 4);
            return buffer;
        }

        [TestCase(1)]
        [TestCase(3)]
        [Category("Flaky")]
        public async Task TracksDisconnectsCorrectly(int total)
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

                    //Assert.True(log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Disposing transport to")) >= currentAttempt - 1);
                }
            }

            await AssertAsync.ThatEventually(() => total == telemetry.TcpConnections.Sum(t => t.Disconnects));
            Assert.AreEqual(1, telemetry.TcpConnections.Count);
            Assert.AreEqual(total, telemetry.TcpConnections.Sum(t => t.Connects));
            Assert.AreEqual(total, telemetry.TcpConnections.Sum(t => t.Disconnects));
        }


        // connects over multiple time slices

        #endregion

        #region TcpStatistics

        #endregion

        #region ApiStatistics

        //[Theory]
        //[TestCase(1)]
        //[TestCase(10)]
        public async Task TracksRequestFailuresCorrectly(int total)
        {
            var aggregationPeriod = TimeSpan.FromMilliseconds(50);
            var telemetry = new TelemetryTracker(aggregationPeriod.Times(100));

            var endpoint = TestConfig.ServerEndpoint();
            var config = telemetry.ToConfiguration(new ConnectionConfiguration(Retry.None, requestTimeout: aggregationPeriod.Times(2)));
            var tasks = new List<Task>();
            for (var i = 0; i < total; i++) {
                tasks.Add(FetchAsync(endpoint, config));
            }
            var requestTasks = Task.WhenAll(tasks);
            await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));

            //var config = telemetry.ToConfiguration(new ConnectionConfiguration(requestTimeout: aggregationPeriod.Times(2)));
            //using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log)) {
            //    var tasks = new List<Task>();
            //    for (var i = 0; i < total; i++) {
            //        tasks.Add(conn.SendAsync(new FetchRequest(), CancellationToken.None));
            //    }
            //    var requestTasks = Task.WhenAll(tasks);
            //    await Task.WhenAny(requestTasks, Task.Delay(aggregationPeriod.Times(5)));
            //}

            Assert.AreEqual(1, telemetry.Requests.Count);
            Assert.AreEqual(0, telemetry.Requests.Sum(t => t.Successes.GetOrDefault(ApiKey.Fetch)));
            Assert.AreEqual(total, telemetry.Requests.Sum(t => t.Attempts.GetOrDefault(ApiKey.Fetch)));
            Assert.AreEqual(total, telemetry.Requests.Sum(t => t.Failures.GetOrDefault(ApiKey.Fetch)));
            Assert.AreEqual(0, telemetry.Requests.Sum(t => t.Successes.Sum(p => p.Value)));
            Assert.AreEqual(total, telemetry.Requests.Sum(t => t.Attempts.Sum(p => p.Value)));
            Assert.AreEqual(total, telemetry.Requests.Sum(t => t.Failures.Sum(p => p.Value)));
        }

        // success
        // multiple time slices ...

        #endregion

    }
}