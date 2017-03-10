using System;
using System.Collections.Immutable;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public class TelemetryTracker : ITrackEvents
    {
        private readonly TimeSpan _aggregationPeriod;
        private readonly int _maxStatistics;

        public TelemetryTracker(TimeSpan aggregationPeriod, int maxStatistics = 10)
        {
            _aggregationPeriod = aggregationPeriod;
            _maxStatistics = maxStatistics;
        }

        public ImmutableList<TcpStatistics> TcpReads => _tcpReads;
        private ImmutableList<TcpStatistics> _tcpReads = ImmutableList<TcpStatistics>.Empty;
        private readonly object _tcpReadLock = new object();
        private TcpStatistics GetTcpRead()
        {
            var stats = GetStatistics(_tcpReads);
            if (stats == null) {
                lock (_tcpReadLock) {
                    stats = GetOrAddStatistics(() => new TcpStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpReads);
                }
            }
            return stats;
        }

        public ImmutableList<TcpStatistics> TcpWrites => _tcpWrites;
        private ImmutableList<TcpStatistics> _tcpWrites = ImmutableList<TcpStatistics>.Empty;
        private readonly object _tcpWriteLock = new object();
        private TcpStatistics GetTcpWrite()
        {
            var stats = GetStatistics(_tcpWrites);
            if (stats == null) {
                lock (_tcpWriteLock) {
                    stats = GetOrAddStatistics(() => new TcpStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpWrites);
                }
            }
            return stats;
        }

        public ImmutableList<ConnectionStatistics> TcpConnections => _tcpConnections;
        private ImmutableList<ConnectionStatistics> _tcpConnections = ImmutableList<ConnectionStatistics>.Empty;
        private readonly object _tcpConnectionLock = new object();
        private ConnectionStatistics GetTcpConnect()
        {
            var stats = GetStatistics(_tcpConnections);
            if (stats == null) {
                lock (_tcpConnectionLock) {
                    stats = GetOrAddStatistics(() => new ConnectionStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpConnections);
                }
            }
            return stats;
        }

        public ImmutableList<ApiStatistics> Requests => _requests;
        private ImmutableList<ApiStatistics> _requests = ImmutableList<ApiStatistics>.Empty;
        private readonly object _apiRequestLock = new object();
        private ApiStatistics GetApiRequests()
        {
            var stats = GetStatistics(_requests);
            if (stats == null) {
                lock (_apiRequestLock) {
                    stats = GetOrAddStatistics(() => new ApiStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _requests);
                }
            }
            return stats;
        }

        private T GetStatistics<T>(ImmutableList<T> telemetry) where T : Statistics
        {
            if (telemetry.IsEmpty) return null;
            var latest = telemetry[telemetry.Count - 1];
            if (DateTimeOffset.UtcNow >= latest.EndedAt) return null;
            return latest;
        }

        private T GetOrAddStatistics<T>(Func<T> producer, ref ImmutableList<T> telemetry) where T : Statistics
        {
            return GetStatistics(telemetry) ?? AddStatistics(producer(), ref telemetry);
        }

        private T AddStatistics<T>(T stats, ref ImmutableList<T> telemetry)
        {
            telemetry = telemetry.Add(stats);
            if (telemetry.Count > _maxStatistics) {
                telemetry = telemetry.RemoveAt(0);
            }
            return stats;
        }

        public void Disconnected(Endpoint endpoint, Exception exception)
        {
            GetTcpConnect().Disconnected();
        }

        public void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            GetTcpConnect().Attempting();
        }

        public void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            GetTcpConnect().Connected(elapsed);
        }

        public void Writing(Endpoint endpoint, ApiKey apiKey)
        {
            GetTcpWrite().Attempt();
            GetApiRequests().Attempt(apiKey);
        }

        public void WritingBytes(Endpoint endpoint, int bytesAvailable)
        {
            GetTcpWrite().Start(bytesAvailable);
        }

        public void WroteBytes(Endpoint endpoint, int bytesAttempted, int bytesWritten, TimeSpan elapsed)
        {
            GetTcpWrite().Partial(bytesAttempted);
        }

        public void Written(Endpoint endpoint, ApiKey apiKey, int bytesWritten, TimeSpan elapsed)
        {
            GetTcpWrite().Success(elapsed, bytesWritten);
            GetApiRequests().Success(apiKey, elapsed);
        }

        public void WriteFailed(Endpoint endpoint, ApiKey apiKey, TimeSpan elapsed, Exception exception)
        {
            GetApiRequests().Failure(apiKey, elapsed);
        }

        public void Reading(Endpoint endpoint, int bytesAvailable)
        {
            GetTcpRead().Attempt(bytesAvailable);
        }

        public void ReadingBytes(Endpoint endpoint, int bytesAvailable)
        {
            GetTcpRead().Start(bytesAvailable);
        }

        public void ReadBytes(Endpoint endpoint, int bytesAttempted, int bytesRead, TimeSpan elapsed)
        {
            GetTcpWrite().Partial(bytesAttempted);
        }

        public void Read(Endpoint endpoint, int bytesRead, TimeSpan elapsed)
        {
            GetTcpRead().Success(elapsed, bytesRead);
        }

        public void ReadFailed(Endpoint endpoint, int bytesAvailable, TimeSpan elapsed, Exception exception)
        {
            GetTcpRead().Failure(elapsed);
        }

        public void ProduceRequestMessages(int messages, int requestBytes, int compressedBytes)
        {
            GetApiRequests().Produce(messages, requestBytes, compressedBytes);
        }
    }
}