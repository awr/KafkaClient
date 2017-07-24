using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Testing;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class ConnectionTests
    {
        #region Construct

        [Test]
        public async Task ShouldStartReadPollingOnConstruction()
        {
            var log = new MemoryLog();
            var endpoint = TestConfig.ServerEndpoint();
            using (new Connection(endpoint, log: log)) {
                await AssertAsync.ThatEventually(() => log.LogEvents.Any(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Connecting to")), log.ToString);
            }
        }

        [Test]
        public void ShouldReportServerUriOnConstruction()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var conn = new Connection(endpoint, log: TestConfig.Log))
            {
                Assert.AreEqual(endpoint, conn.Endpoint);
            }
        }

        [Test]
        public async Task ThrowsConnectionExceptionOnInvalidEndpoint()
        {
            var options = new KafkaOptions(new Uri("tcp://notadomain"));
            await AssertAsync.Throws<ConnectionException>(() => options.CreateConnectionAsync());
        }

        #endregion 

        #region Connection

        [Test]
        public async Task ShouldStartDedicatedThreadOnConstruction()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            var endpoint = TestConfig.ServerEndpoint();
            using (new Connection(endpoint, config, log: TestConfig.Log)) {
                await AssertAsync.ThatEventually(() => count > 0, () => $"count {count}");
            }
        }

        [Test]
        public async Task ShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            using (var conn = new Connection(TestConfig.ServerEndpoint(), config, TestConfig.Log))
            {
                var task = conn.SendAsync(new FetchRequest(), CancellationToken.None); //will force a connection
                await AssertAsync.ThatEventually(() => count > 1, TimeSpan.FromSeconds(10), () => $"count {count}");
            }
        }

        #endregion

        #region Dispose

        [Test]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            {
                var conn = new Connection(endpoint, log: TestConfig.Log);
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                using (conn) { }
            }
        }

        [Test]
        public async Task ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var conn = new Connection(endpoint, log: TestConfig.Log))
            {
                var task = conn.SendAsync(new MetadataRequest(), CancellationToken.None);
                await Task.WhenAny(task, Task.Delay(1000)).ConfigureAwait(false);
                Assert.False(task.IsCompleted, "The send task should still be pending.");
            }
        }

        #endregion

        #region Read

        [Test]
        public async Task ShouldLogDisconnectAndRecover()
        {
            var log = new MemoryLog();
            var clientDisconnected = 0;
            var clientConnected = 0;
            var serverConnected = 0;

            var config = new ConnectionConfiguration(
                onDisconnected: (e, exception) => {
                    Interlocked.Increment(ref clientDisconnected);
                },
                onConnected: (e, attempt, elapsed) => {
                    Interlocked.Increment(ref clientConnected);
                });

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log) {
                OnConnected = () => Interlocked.Increment(ref serverConnected)
            })
            using (new Connection(endpoint, config, log: log))
            {
                for (var connectionAttempt = 1; connectionAttempt <= 4; connectionAttempt++)
                {
                    var currentAttempt = connectionAttempt;
                    await AssertAsync.ThatEventually(() => serverConnected == currentAttempt, () => $"server {serverConnected}, attempt {currentAttempt}");
                    await server.SendDataAsync(new ArraySegment<byte>(CreateCorrelationMessage(connectionAttempt)));
                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Sent CONNECTION attempt {currentAttempt}"));
                    await AssertAsync.ThatEventually(() => clientConnected == currentAttempt, TimeSpan.FromMilliseconds(200), () => $"client {clientConnected}, attempt {currentAttempt}");

                    await AssertAsync.ThatEventually(() => log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Received 4 bytes (id ")) == currentAttempt, () => $"attempt {currentAttempt}\n" + log.ToString(LogLevel.Info));

                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Dropping CONNECTION attempt {currentAttempt}"));
                    server.DropConnection();
                    await AssertAsync.ThatEventually(() => clientDisconnected == currentAttempt, () => $"client {clientDisconnected}, attempt {currentAttempt}");

                    Assert.True(log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Disposing transport to")) >= currentAttempt);
                }
            }
        }

        [Test]
        public async Task ShouldFinishPartiallyReadMessage()
        {
            var log = new MemoryLog();
            var bytesRead = 0;
            var firstLength = 99;

            var config = new ConnectionConfiguration(onReadBytes: (e, attempted, actual, elapsed) => {
                if (Interlocked.Add(ref bytesRead, actual) == firstLength) throw new OutOfMemoryException();
            });

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (new Connection(endpoint, config, log))
            {
                // send size
                var size = 200;
                await server.SendDataAsync(new ArraySegment<byte>(size.ToBytes()));
                var bytes = new byte[size];
                new Random(42).NextBytes(bytes);
                var offset = 0;
                var correlationId = 200;
                foreach (var b in correlationId.ToBytes()) {
                    bytes[offset++] = b;
                }

                // send half of payload
                await server.SendDataAsync(new ArraySegment<byte>(bytes, 0, firstLength));
                await AssertAsync.ThatEventually(() => bytesRead >= firstLength, () => $"read {bytesRead}, length {bytes.Length}");

                Assert.AreEqual(1, log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith($"Unexpected response (id {correlationId}, {size}? bytes) from")));
                Assert.AreEqual(0, log.LogEvents.Count(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith($"Received {size} bytes (id {correlationId})")));

                // send half of payload should be skipped
                while (!await server.SendDataAsync(new ArraySegment<byte>(bytes, firstLength, size - firstLength))) {
                    // repeat until the connection is all up and working ...
                }
                await AssertAsync.ThatEventually(() => bytesRead >= size, () => $"read {bytesRead}, size {size}");
                await AssertAsync.ThatEventually(() => log.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith($"Received {size} bytes (id {correlationId})")) == 1, log.ToString);
            }
        }

        [Test]
        public async Task ShouldNotFinishPartiallyReadMessage()
        {
            var log = new MemoryLog();
            var bytesRead = 0;

            var config = new ConnectionConfiguration(onReadBytes: (e, attempted, actual, elapsed) => Interlocked.Add(ref bytesRead, actual));

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (new Connection(endpoint, config, log))
            {
                // send size
                var size = 200;
                await server.SendDataAsync(new ArraySegment<byte>(size.ToBytes()));
                var bytes = new byte[99];
                new Random(42).NextBytes(bytes);
                var offset = 0;
                var correlationId = 200;
                foreach (var b in correlationId.ToBytes()) {
                    bytes[offset++] = b;
                }

                // send half of payload
                await server.SendDataAsync(new ArraySegment<byte>(bytes, 0, 99));
                await AssertAsync.ThatEventually(() => bytesRead >= bytes.Length, () => $"read {bytesRead}, length {bytes.Length}");

                Assert.AreEqual(1, log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith($"Unexpected response (id {correlationId}, {size}? bytes) from")));
                Assert.AreEqual(0, log.LogEvents.Count(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith($"Received {size} bytes (id {correlationId})")));

                server.DropConnection();

                await AssertAsync.ThatEventually(() => log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Socket has been re-established, so recovery of the remaining of the current message is uncertain at best")) == 1, log.ToString);
            }
        }

        [Test]
        public async Task ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;
            var onRead = 0;

            var log = new MemoryLog();

            var config = new ConnectionConfiguration(onRead: (e, buffer, elapsed) => Interlocked.Increment(ref onRead));
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, config, log: log))
            {
                //send correlation message
                await server.SendDataAsync(new ArraySegment<byte>(CreateCorrelationMessage(correlationId)));

                //wait for connection
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));
                await AssertAsync.ThatEventually(() => onRead > 0, () => $"read attempts {onRead}");

                // shortly after receivedData, but still after
                await AssertAsync.ThatEventually(() => log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message == $"Unexpected response (id {correlationId}, 4? bytes) from {endpoint}"), log.ToString);
            }
        }

        [Test]
        public async Task ReadShouldCancelWhileAwaitingResponse()
        {
            var count = 0;
            var semaphore = new SemaphoreSlim(0);
            var config = new ConnectionConfiguration(onReadingBytes: (e, available) => {
                Interlocked.Increment(ref count);
                semaphore.Release();
                
            });

            var endpoint = TestConfig.ServerEndpoint();
            using (new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, config, TestConfig.Log)) {
                var token = new CancellationTokenSource();

                var taskResult = conn.SendAsync(new FetchRequest(), token.Token);

                await Task.Delay(100);
                token.Cancel();

                semaphore.Wait(TimeSpan.FromSeconds(1));
                Assert.True(count >= 1, "Read should have cancelled and incremented count.");
                await AssertAsync.ThatEventually(() => taskResult.IsCanceled);
            }
        }

        [Test]
        public async Task ReadShouldCancelWhileAwaitingReconnection()
        {
            using (var token = new CancellationTokenSource()) {
                var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => token.Cancel());
                var endpoint = TestConfig.ServerEndpoint();
                using (var conn = new Connection(endpoint, config, TestConfig.Log)) {
                    var taskResult = conn.SendAsync(new FetchRequest(), token.Token);
                    await Task.WhenAny(taskResult, Task.Delay(500));
                    Assert.True(taskResult.IsCanceled);
                }
            }
        }

        [Test]
        public async Task ReadShouldReconnectEvenAfterCancelledRead()
        {
            using (var token = new CancellationTokenSource()) {
                var connectionAttempt = 0;
                var config = new ConnectionConfiguration(
                    onConnecting: (e, attempt, elapsed) => {
                        if (Interlocked.Increment(ref connectionAttempt) > 1) {
                            token.Cancel();
                        }
                    });
                var endpoint = TestConfig.ServerEndpoint();
                using (var conn = new Connection(endpoint, config, TestConfig.Log)) {
                    var taskResult = conn.SendAsync(new FetchRequest(), token.Token);
                    await AssertAsync.ThatEventually(() => connectionAttempt > 1, () => $"attempt {connectionAttempt}");
                }
            }
        }

        [Test]
        public async Task ShouldReconnectAfterLosingConnectionAndBeAbleToStartNewRead()
        {
            var log = TestConfig.Log;
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log)) {
                var serverDisconnects = 0;
                var serverConnects = 0;
                var clientDisconnects = 0;
                var clientReads = 0;
                var clientBytesRead = 0;

                server.OnConnected = () => Interlocked.Increment(ref serverConnects);
                server.OnDisconnected = () => Interlocked.Increment(ref serverDisconnects);
                var config = new ConnectionConfiguration(
                    onDisconnected: (e, exception) => Interlocked.Increment(ref clientDisconnects),
                    onReading:(e, available) => Interlocked.Increment(ref clientReads),
                    onRead: (e, read, elapsed) => Interlocked.Add(ref clientBytesRead, read));
                using (var conn = new Connection(endpoint, config, log)) {
                    await AssertAsync.ThatEventually(() => serverConnects > 0, () => $"connects {serverConnects}");
                    await AssertAsync.ThatEventually(() => clientReads > 0, TimeSpan.FromSeconds(1), () => $"reads {clientReads}");

                    server.DropConnection();

                    await AssertAsync.ThatEventually(() => clientDisconnects > 0, TimeSpan.FromSeconds(10), () => $"disconnects {clientDisconnects}");
                    Assert.AreEqual(0, clientBytesRead); // client should not have received any bytes

                    await AssertAsync.ThatEventually(() => serverConnects == 2, TimeSpan.FromSeconds(6), () => $"connects {serverConnects}");

                    await server.SendDataAsync(new ArraySegment<byte>(8.ToBytes()));
                    await server.SendDataAsync(new ArraySegment<byte>(99.ToBytes()));
                    await AssertAsync.ThatEventually(() => clientBytesRead == 8, TimeSpan.FromSeconds(1), () => $"bytes read {clientBytesRead}");
                }
            }
        }

        #endregion

        #region Send

        [Test]
        public async Task SendAsyncShouldTimeoutWhenSendAsyncTakesTooLong()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(1)), log: TestConfig.Log))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                var sendTask = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                await Task.WhenAny(sendTask, Task.Delay(100));

                Assert.True(sendTask.IsFaulted, "Task should have reported an exception.");
                Assert.IsInstanceOf<TimeoutException>(sendTask.Exception.InnerException);
            }
        }

        [Test]
        public async Task SendAsyncShouldReturnImmediatelyWhenNoAcks()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(1)), log: TestConfig.Log))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                var sendTask = conn.SendAsync(new ProduceRequest(new ProduceRequest.Topic("topic", 0, new []{ new Message("value") }), acks: 0), CancellationToken.None);

                await Task.WhenAny(sendTask, Task.Delay(100));

                Assert.False(sendTask.IsFaulted);
                Assert.True(sendTask.IsCompleted);
            }
        }

        [Test]
        public async Task SendAsyncShouldNotAllowResponseToTimeoutWhileAwaitingKafkaToEstableConnection()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000)), log: TestConfig.Log))
            {
                // SendAsync blocked by reconnection attempts
                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                // Task result should be WaitingForActivation
                Assert.False(taskResult.IsFaulted);
                Assert.AreEqual(TaskStatus.WaitingForActivation, taskResult.Status);

                // Starting server to establish connection
                using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
                {
                    server.OnConnected = () => TestConfig.Log.Info(() => LogEvent.Create("Client connected..."));
                    server.OnReceivedAsync = async data => {
                        var requestContext = KafkaDecoder.DecodeHeader(data.Skip(Request.IntegerByteSize));
                        await server.SendDataAsync(MessageHelper.CreateMetadataResponse(requestContext, "Test"));
                    };

                    await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(5)));

                    Assert.False(taskResult.IsFaulted);
                    Assert.False(taskResult.IsCanceled);
                    await taskResult;
                    Assert.AreEqual(TaskStatus.RanToCompletion, taskResult.Status);
                }
            }
        }

        [Test]
        public void DynamicVersionUsesFallbackVersion()
        {
            var dynamicVersion = VersionSupport.Kafka10.Dynamic();
            var staticVersion = VersionSupport.Kafka10;
            foreach (ApiKey apiKey in Enum.GetValues(typeof (ApiKey))) {
                Assert.AreEqual(staticVersion.GetVersion(apiKey), dynamicVersion.GetVersion(apiKey));
            }
        }

        [Test]
        public async Task SendAsyncShouldUseStatictVersionInfo()
        {
            IRequestContext context = null;
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000), versionSupport: VersionSupport.Kafka10), log: TestConfig.Log))
            {
                server.OnReceivedAsync = async data => {
                    context = KafkaDecoder.DecodeHeader(data.Skip(Request.IntegerByteSize));
                    await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new FetchResponse()));
                };

                await conn.SendAsync(new FetchRequest(new FetchRequest.Topic("Foo", 0, 0)), CancellationToken.None);
                await AssertAsync.ThatEventually(() => context != null && context.ApiVersion.GetValueOrDefault() == 2, () => $"version {context?.ApiVersion}");
            }
        }

        [Test]
        public async Task SendAsyncWithDynamicVersionInfoMakesVersionCallFirst()
        {
            var firstCorrelation = -1;
            var correlationId = 0;
            var sentVersion = (short)-1;

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(3), versionSupport: VersionSupport.Kafka8.Dynamic()), log: TestConfig.Log))
            {
                var apiVersion = (short)3;
                server.OnReceivedAsync = async data => {
                    var context = KafkaDecoder.DecodeHeader(data.Skip(Request.IntegerByteSize));
                    if (firstCorrelation < 0)
                    {
                        firstCorrelation = context.CorrelationId;
                    }
                    correlationId = context.CorrelationId;
                    switch (correlationId - firstCorrelation)
                    {
                        case 0:
                            await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new ApiVersionsResponse(ErrorCode.NONE, new[] { new ApiVersionsResponse.VersionSupport(ApiKey.Fetch, apiVersion, apiVersion) })));
                            break;
                        case 1:
                            sentVersion = context.ApiVersion.GetValueOrDefault();
                            await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new FetchResponse()));
                            break;

                        default:
                            return;
                    }
                };

                await conn.SendAsync(new FetchRequest(new FetchRequest.Topic("Foo", 0, 0)), CancellationToken.None);
                await AssertAsync.ThatEventually(() => correlationId - firstCorrelation >= 1, () => $"first {firstCorrelation}, current {correlationId}");
                Assert.AreEqual(apiVersion, sentVersion);
            }
        }

        [Test]
        public async Task SendAsyncWithDynamicVersionInfoOnlyMakesVersionCallOnce()
        {
            var versionRequests = 0;

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(3), versionSupport: VersionSupport.Kafka8.Dynamic()), log: TestConfig.Log))
            {
                server.OnReceivedAsync = async data => {
                    var fullHeader = KafkaDecoder.DecodeFullHeader(data.Skip(Request.IntegerByteSize));
                    var context = fullHeader.Item1;
                    switch (fullHeader.Item2) {
                        case ApiKey.ApiVersions:
                            Interlocked.Increment(ref versionRequests);
                            await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new ApiVersionsResponse(ErrorCode.NONE, new[] { new ApiVersionsResponse.VersionSupport(ApiKey.Fetch, 3, 3) })));
                            break;

                        default:
                            await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new FetchResponse()));
                            break;
                    }
                };

                for (var i = 0; i < 3; i++) {
                    await conn.SendAsync(new FetchRequest(new FetchRequest.Topic("Foo", 0, 0)), CancellationToken.None);
                }

                Assert.AreEqual(1, versionRequests);
            }
        }

        [Test]
        public async Task SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(100)), log: TestConfig.Log))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                var tasks = new[] {
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None)
                };

                await AssertAsync.ThatEventually(() => tasks.All(t => t.IsFaulted));
                foreach (var task in tasks) {
                    Assert.True(task.IsFaulted, "Task should have faulted.");
                    Assert.IsInstanceOf<TimeoutException>(task.Exception.InnerException);
                }
            }
        }

        [Test]
        public async Task MessagesStillLogWhenSendTimesOut()
        {
            var logger = new MemoryLog();
            var received = 0;
            var timeout = TimeSpan.FromMilliseconds(100);

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: timeout, onRead: (e, read, elapsed) => Interlocked.Increment(ref received)), logger))
            {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                server.OnReceivedAsync = async data => {
                    var context = KafkaDecoder.DecodeHeader(data.Skip(Request.IntegerByteSize));
                    await Task.Delay(timeout);
                    await server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new MetadataResponse()));
                };

                await AssertAsync.Throws<TimeoutException>(() => conn.SendAsync(new MetadataRequest(), CancellationToken.None));
                await AssertAsync.ThatEventually(() => received > 0, () => $"received {received}");
                await AssertAsync.ThatEventually(() => logger.LogEvents.Any(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith("Timed out -----> (timed out or otherwise errored in client)")), () => logger.ToString(LogLevel.Debug));
            }
        }

        [Test]
        public async Task TimedOutQueueIsClearedWhenTooBig()
        {
            var logger = new MemoryLog();
            var timeout = TimeSpan.FromMilliseconds(1);

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: timeout), logger)) {
                await Task.WhenAny(server.ClientConnected, Task.Delay(TimeSpan.FromSeconds(3)));

                await AssertAsync.Throws<TimeoutException>(() => Task.WhenAll(Enumerable.Range(0, 201).Select(i => conn.SendAsync(new MetadataRequest(), CancellationToken.None))));
                await AssertAsync.ThatEventually(() => logger.LogEvents.Any(e => e.Item1 == LogLevel.Debug && e.Item2.Message.StartsWith("Clearing timed out requests to avoid overflow")), () => logger.ToString(LogLevel.Debug));
            }
        }

        [Test]
        public async Task CorrelationOverflowGuardWorks()
        {
            var correlationId = -1;

            var endpoint = TestConfig.ServerEndpoint();
            using (var server = new TcpServer(endpoint.Ip.Port, TestConfig.Log))
            using (var conn = new Connection(endpoint, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(5)), TestConfig.Log)) {
                server.OnReceivedAsync = data => {
                    var context = KafkaDecoder.DecodeHeader(data.Skip(Request.IntegerByteSize));
                    correlationId = context.CorrelationId;
                    return Task.FromResult(0);
                };

                try {
                    Connection.OverflowGuard = 10;
                    await AssertAsync.Throws<TimeoutException>(() => conn.SendAsync(new MetadataRequest(), CancellationToken.None));
                    var initialCorrelation = correlationId;
                    await AssertAsync.Throws<TimeoutException>(() => Task.WhenAll(Enumerable.Range(initialCorrelation, Connection.OverflowGuard - 1).Select(i => conn.SendAsync(new MetadataRequest(), CancellationToken.None))));
                    await AssertAsync.ThatEventually(() => correlationId > 1, () => $"correlation {correlationId}");
                    var currentCorrelation = correlationId;
                    await AssertAsync.Throws<TimeoutException>(() => Task.WhenAll(Enumerable.Range(0, Connection.OverflowGuard / 2).Select(i => conn.SendAsync(new MetadataRequest(), CancellationToken.None))));
                    await AssertAsync.ThatEventually(() => correlationId < currentCorrelation, () => $"correlation {correlationId}");
                }
                finally {
                    Connection.OverflowGuard = int.MaxValue >> 1;
                }
            }
        }

        #endregion

        private static byte[] CreateCorrelationMessage(int id)
        {
            var buffer = new byte[8];
            var stream = new MemoryStream(buffer);
            stream.Write(Request.CorrelationSize.ToBytes(), 0, 4);
            stream.Write(id.ToBytes(), 0, 4);
            return buffer;
        }
    }
}