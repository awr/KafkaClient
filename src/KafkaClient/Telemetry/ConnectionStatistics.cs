using System;
using System.Threading;

namespace KafkaClient.Telemetry
{
    public class ConnectionStatistics : Statistics
    {
        public ConnectionStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
        }

        private int _attempts;
        public int Attempts => _attempts;

        public void Attempting()
        {
            Interlocked.Increment(ref _attempts);
        }

        private int _connects;
        public int Connects => _connects;

        private long _duration;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Connected(TimeSpan duration)
        {
            Interlocked.Increment(ref _connects);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _disconnects;
        public int Disconnects => _disconnects;

        public void Disconnected()
        {
            Interlocked.Increment(ref _disconnects);
        }
    }
}