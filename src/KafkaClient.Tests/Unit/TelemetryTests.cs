using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Telemetry;
using NUnit.Framework;

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
            using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log))
            {
                var task = conn.SendAsync(new FetchRequest(), CancellationToken.None); // will force a connection
                await Task.WhenAny(task, Task.Delay(aggregationPeriod.Times(5)));
            }

            Assert.That(telemetry.TcpConnections.Count, Is.EqualTo(total));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Connects), Is.EqualTo(0));
            Assert.That(telemetry.TcpConnections.Sum(t => t.Attempts), Is.EqualTo(total));
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

        #endregion

    }
}