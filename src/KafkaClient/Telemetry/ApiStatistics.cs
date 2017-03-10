using System;
using System.Collections.Concurrent;
using System.Threading;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public class ApiStatistics : Statistics
    {
        public ApiStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
            Attempts = new ConcurrentDictionary<ApiKey, int>();
            Successes = new ConcurrentDictionary<ApiKey, int>();
            Failures = new ConcurrentDictionary<ApiKey, int>();
        }

        public ConcurrentDictionary<ApiKey, int> Attempts { get; }

        public void Attempt(ApiKey apiKey)
        {
            Attempts.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
        }

        public ConcurrentDictionary<ApiKey, int> Successes { get; }

        private long _duration;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(ApiKey apiKey, TimeSpan duration)
        {
            Successes.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        public ConcurrentDictionary<ApiKey, int> Failures { get; }

        public void Failure(ApiKey apiKey, TimeSpan duration)
        {
            Failures.AddOrUpdate(apiKey, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _messages;
        public int Messages => _messages;
        private int _messageBytes;
        public int MessageBytes => _messageBytes;
        private int _messageTcpBytes;
        public int MessageTcpBytes => _messageTcpBytes;

        public void Produce(int messages, int wireBytes, int bytesCompressed)
        {
            Interlocked.Add(ref _messages, messages);
            Interlocked.Add(ref _messageTcpBytes, wireBytes);
            Interlocked.Add(ref _messageBytes, wireBytes + bytesCompressed);
        }
    }
}