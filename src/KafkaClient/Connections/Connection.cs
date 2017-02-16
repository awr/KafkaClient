using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    /// <summary>
    /// The SendAsync function will return a Task and complete once the data has been sent to the outbound stream.
    /// The Read response is handled by a single thread polling the stream for data and matching the sent requests by 
    /// correlation id.
    /// </summary>
    public class Connection : IConnection
    {
        private readonly ConcurrentDictionary<int, AsyncItem> _requestsByCorrelation = new ConcurrentDictionary<int, AsyncItem>();
        private readonly ConcurrentDictionary<int, AsyncItem> _timedOutRequestsByCorrelation = new ConcurrentDictionary<int, AsyncItem>();
        private readonly ILog _log;
        private readonly ITransport _transport;
        private readonly IConnectionConfiguration _configuration;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount; // = 0
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        public bool IsDisposed => _disposeCount > 0;

        private readonly Task _receiveTask;
        private int _receiveCount;
        private static int _correlationIdSeed;

        private readonly SemaphoreSlim _versionSupportSemaphore = new SemaphoreSlim(1, 1);
        private IVersionSupport _versionSupport; // = null

        /// <summary>
        /// Initializes a new instance of the Connection class.
        /// </summary>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        /// <param name="configuration">The configuration, including connection and request timeouts.</param>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        public Connection(Endpoint endpoint, IConnectionConfiguration configuration = null, ILog log = null)
        {
            Endpoint = endpoint;
            _configuration = configuration ?? ConnectionConfiguration.Default;
            _log = log ?? TraceLog.Log;

            _transport = _configuration.SslConfiguration == null
                ? (ITransport)new SocketTransport(endpoint, _configuration, _log) 
                : new SslTransport(endpoint, _configuration, _log);

            // This thread will poll the receive stream for data, parse a message out
            // and trigger an event with the message payload
            _receiveTask = Task.Factory.StartNew(DedicatedReceiveAsync, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        /// <inheritdoc />
        public Endpoint Endpoint { get; }

        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IRequest to send to the kafka servers.</param>
        /// <param name="context">The context for the request.</param>
        /// <param name="cancellationToken">Cancellation token used to cancel the transfer.</param>
        /// <returns></returns>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse
        {
            var version = context?.ApiVersion;
            if (!version.HasValue) {
                version = await GetVersionAsync(request.ApiKey, cancellationToken).ConfigureAwait(false);
            }
            context = RequestContext.Copy(context, NextCorrelationId(), version, encoders: _configuration.Encoders, onProduceRequestMessages: _configuration.OnProduceRequestMessages);

            ArraySegment<byte> receivedBytes;
            using (var asyncItem = new AsyncItem(context, request)) {
                if (request.ExpectResponse) {
                    AddToCorrelationMatching(asyncItem);
                }

                using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeToken.Token)) {
                    var timer = new Stopwatch();
                    try {
                        await _transport.ConnectAsync(cancellation.Token).ConfigureAwait(false);
                        var item = asyncItem;
                        _log.Info(() => LogEvent.Create($"Sending {request.ShortString()} (id {context.CorrelationId}, {item.RequestBytes.Count} bytes) to {Endpoint}"));
                        _log.Debug(() => LogEvent.Create($"{request.ApiKey} -----> {Endpoint} {{Context:{context},\nRequest:{request}}}"));
                        _configuration.OnWriting?.Invoke(Endpoint, request.ApiKey);
                        timer.Start();
                        var bytesWritten = await _transport.WriteBytesAsync(asyncItem.RequestBytes, cancellation.Token, context.CorrelationId).ConfigureAwait(false);
                        timer.Stop();
                        _configuration.OnWritten?.Invoke(Endpoint, request.ApiKey, bytesWritten, timer.Elapsed);

                        if (!request.ExpectResponse) return default (T);
                    } catch (Exception ex) {
                        timer.Stop();
                        _configuration.OnWriteFailed?.Invoke(Endpoint, request.ApiKey, timer.Elapsed, ex);
                        RemoveFromCorrelationMatching(asyncItem, ex);
                        throw;
                    }

                    receivedBytes = await asyncItem.ReceiveTask.Task.ThrowIfCancellationRequested(cancellation.Token).ConfigureAwait(false);
                }
            }

            var response = request.ToResponse(context, receivedBytes);
            _log.Debug(() => LogEvent.Create($"{Endpoint} -----> {{Context:{context},\n{request.ApiKey}Response:{response}}}"));
            return response;
        }

        private async Task<short> GetVersionAsync(ApiKey apiKey, CancellationToken cancellationToken)
        {
            var configuredSupport = _configuration.VersionSupport as DynamicVersionSupport;
            if (configuredSupport == null) return _configuration.VersionSupport.GetVersion(apiKey).GetValueOrDefault(); 

            var versionSupport = _versionSupport;
            if (versionSupport != null) return versionSupport.GetVersion(apiKey).GetValueOrDefault();

            return await _versionSupportSemaphore.LockAsync(
                async () => {
                    try {
                        return await _configuration.ConnectionRetry.TryAsync(
                            async (retryAttempt, elapsed) => {
                                var response = await SendAsync(new ApiVersionsRequest(), cancellationToken, new RequestContext(version: 0)).ConfigureAwait(false);
                                if (response.error_code.IsRetryable()) return RetryAttempt<short>.Retry;
                                if (!response.error_code.IsSuccess()) return RetryAttempt<short>.Abort;

                                var supportedVersions = response.api_versions.ToImmutableDictionary(
                                                                _ => _.api_key,
                                                                _ => configuredSupport.UseMaxSupported ? _.max_version : _.min_version);
                                _versionSupport = new VersionSupport(supportedVersions);
                                return new RetryAttempt<short>(_versionSupport.GetVersion(apiKey).GetValueOrDefault());
                            },
                            (exception, retryAttempt, retryDelay) => {
                                _log.Debug(() => LogEvent.Create(exception, $"Failed getting version info on retry {retryAttempt}: Will retry in {retryDelay}"));
                                exception.PrepareForRethrow();
                            },
                            () => _versionSupport = _configuration.VersionSupport, // fall back to default support in this case
                            cancellationToken);
                    } catch (Exception ex) {
                        _log.Error(LogEvent.Create(ex));
                        _versionSupport = _configuration.VersionSupport; // fall back to default support in this case
                        return (short)0;
                    }
                }, 
                cancellationToken
            ).ConfigureAwait(false);
        }

        private async Task DedicatedReceiveAsync()
        {
            // only allow one reader to execute, dump out all other requests
            if (Interlocked.Increment(ref _receiveCount) > 1) return;

            try {
                var header = new byte[Request.IntegerByteSize + Request.CorrelationSize];
                AsyncItem asyncItem = null;
                while (!_disposeToken.IsCancellationRequested) {
                    await Retry // use backoff so we don't take over the CPU when there's a failure
                        .WithBackoff(int.MaxValue, minimumDelay: TimeSpan.FromMilliseconds(5), maximumDelay: TimeSpan.FromSeconds(5))
                        .TryAsync(
                            async (retryAttempt, elapsed) => {
                                await _transport.ConnectAsync(_disposeToken.Token).ConfigureAwait(false);

                                // reading the header
                                if (asyncItem == null) {
                                    await _transport.ReadBytesAsync(new ArraySegment<byte>(header), _disposeToken.Token).ConfigureAwait(false);
                                    var responseSize = BitConverter.ToInt32(header, 0).ToBigEndian();
                                    var correlationId = BitConverter.ToInt32(header, Request.IntegerByteSize).ToBigEndian();

                                    asyncItem = LookupByCorrelateId(correlationId, responseSize);
                                    if (asyncItem.ResponseBytes.Count > 0) {
                                        _log.Error(LogEvent.Create($"Request id {correlationId} matched a previous response ({asyncItem.ResponseBytes.Count + Request.CorrelationSize} of {responseSize + Request.CorrelationSize} bytes), now overwriting with {responseSize}? bytes"));
                                    }
                                    asyncItem.ResponseBytes = new ArraySegment<byte>(new byte[responseSize - Request.CorrelationSize]);
                                }

                                // reading the body
                                await _transport.ReadBytesAsync(asyncItem.ResponseBytes, CancellationToken.None).ConfigureAwait(false);
                                asyncItem.ResponseCompleted(_log);
                                asyncItem = null;

                                return new RetryAttempt<bool>(true);
                            },
                            (exception, retryAttempt, retryDelay) => {
                                if (_disposeToken.IsCancellationRequested) {
                                    exception.PrepareForRethrow();
                                    _disposeToken.Token.ThrowIfCancellationRequested(); // in case exception is null
                                }

                                if (exception is ConnectionException) {
                                    _log.Warn(() => LogEvent.Create(exception, "Socket has been re-established, so recovery of the remaining of the current message is uncertain at best"));
                                    try {
                                        asyncItem.ResponseCompleted(_log);
                                    } catch {
                                        // ignore
                                    }
                                    asyncItem = null;
                                }

                                _log.Write(retryAttempt == 0 ? LogLevel.Error : LogLevel.Info, () => LogEvent.Create(exception, $"Failed receiving from {Endpoint} on retry {retryAttempt}: Will retry in {retryDelay}"));
                            },
                            _disposeToken.Token).ConfigureAwait(false);
                }
            } catch (Exception ex) {
                _log.Debug(() => LogEvent.Create(ex));
            } finally {
                await DisposeAsync().ConfigureAwait(false);
                Interlocked.Decrement(ref _receiveCount);
                _log.Info(() => LogEvent.Create($"Stopped receiving from {Endpoint}"));
            }
        }

        #region Correlation

        private AsyncItem LookupByCorrelateId(int correlationId, int expectedBytes)
        {
            AsyncItem asyncItem;
            if (_requestsByCorrelation.TryRemove(correlationId, out asyncItem) || _timedOutRequestsByCorrelation.TryRemove(correlationId, out asyncItem)) {
                _log.Debug(() => LogEvent.Create($"Matched {asyncItem.ApiKey} response (id {correlationId}, {expectedBytes}? bytes) from {Endpoint}"));
                return asyncItem;
            }

            _log.Warn(() => LogEvent.Create($"Unexpected response (id {correlationId}, {expectedBytes}? bytes) from {Endpoint}"));
            return new AsyncItem(new RequestContext(correlationId), new UnknownRequest());
        }

        internal static int OverflowGuard = int.MaxValue >> 1;
        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationIdSeed);
            if (id >= OverflowGuard) {
                // to avoid integer overflow
                Interlocked.Exchange(ref _correlationIdSeed, 0);
            }
            return id;
        }

        private void AddToCorrelationMatching(AsyncItem asyncItem)
        {
            if (asyncItem == null) return;
            if (_requestsByCorrelation.TryAdd(asyncItem.Context.CorrelationId, asyncItem) == false) {
                throw new KafkaException("Failed to register request for async response.");
            }
            asyncItem.OnTimedOut(RemoveFromCorrelationMatching, _configuration.RequestTimeout);
        }

        private void RemoveFromCorrelationMatching(AsyncItem asyncItem, Exception exception = null)
        {
            if (asyncItem == null) return;

            var correlationId = asyncItem.Context.CorrelationId;
            AsyncItem request;
            if (_requestsByCorrelation.TryRemove(correlationId, out request)) {
                _log.Info(() => LogEvent.Create($"Removed request {request.ApiKey} (id {correlationId}): timed out or otherwise errored in client."));
                if (_timedOutRequestsByCorrelation.Count > 100) {
                    _log.Debug(() => LogEvent.Create($"Clearing timed out requests to avoid overflow ({_timedOutRequestsByCorrelation.Count})."));
                    _timedOutRequestsByCorrelation.Clear();
                }
                _timedOutRequestsByCorrelation.TryAdd(correlationId, request);
            }

            if (_disposeToken.IsCancellationRequested) {
                asyncItem.ReceiveTask.TrySetException(new ObjectDisposedException("The object is being disposed and the connection is closing."));
            } else if (exception != null) {
                asyncItem.ReceiveTask.TrySetException(exception);
            } else {
                asyncItem.ReceiveTask.TrySetException(new TimeoutException($"Timeout expired after {_configuration.RequestTimeout.TotalMilliseconds} ms."));
            }
        }

        #endregion

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) > 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                _log.Debug(() => LogEvent.Create("Disposing Connection"));
                _disposeToken.Cancel();
                await Task.WhenAny(_receiveTask, Task.Delay(1000)).ConfigureAwait(false);

                using (_disposeToken) {
                    _transport.Dispose();
                    _versionSupportSemaphore.Dispose();
                    _timedOutRequestsByCorrelation.Clear();
                }
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        private class UnknownRequest : IRequest
        {
            public bool ExpectResponse => true;
            public ApiKey ApiKey => ApiKey.ApiVersions;
            public string ShortString() => "Unknown";

            public ArraySegment<byte> ToBytes(IRequestContext context)
            {
                throw new NotImplementedException();
            }
        }

        private class AsyncItem : IDisposable
        {
            private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
            private CancellationTokenRegistration _registration;

            public AsyncItem(IRequestContext context, IRequest request)
            {
                Context = context;
                Request = request;
                RequestBytes = request is UnknownRequest ? new ArraySegment<byte>() : request.ToBytes(context);
                ApiKey = request.ApiKey;
                ReceiveTask = new TaskCompletionSource<ArraySegment<byte>>();
            }

            public IRequestContext Context { get; }
            private IRequest Request { get; } // for debugging
            public ApiKey ApiKey { get; }
            public ArraySegment<byte> RequestBytes { get; }
            public TaskCompletionSource<ArraySegment<byte>> ReceiveTask { get; }
            public ArraySegment<byte> ResponseBytes { get; internal set;  }

            internal void ResponseCompleted(ILog log)
            {
                if (Request is UnknownRequest) {
                    log.Info(() => LogEvent.Create($"Received {ResponseBytes.Count + Protocol.Request.CorrelationSize} bytes (id {Context.CorrelationId})"));
                    return;
                }
                log.Info(() => LogEvent.Create($"Received {ApiKey} response (id {Context.CorrelationId}, {ResponseBytes.Count + Protocol.Request.CorrelationSize} bytes)"));
                if (!ReceiveTask.TrySetResult(ResponseBytes)) {
                    log.Debug(() => {
                        var result = ApiKey.ToResponse(Context, ResponseBytes);
                        return LogEvent.Create($"Timed out -----> (timed out or otherwise errored in client) {{Context:{Context},\n{ApiKey}Response:{result}}}");
                    });
                }
            }

            public void OnTimedOut(Action<AsyncItem, Exception> cleanupFunction, TimeSpan timeout)
            {
                _registration = _cancellationTokenSource.Token.Register(() => cleanupFunction(this, null));
                _cancellationTokenSource.CancelAfter(timeout);
            }

            public void Dispose()
            {
                using (_cancellationTokenSource) {
                    using (_registration) {
                    }
                }
            }
        }
    }
}