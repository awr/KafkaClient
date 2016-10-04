﻿using System;

namespace KafkaClient.Common
{
    public class BackoffRetry : Retry
    {
        private readonly TimeSpan? _maxDelay;
        private readonly TimeSpan _delay;
        private readonly bool _isLinear;

        public BackoffRetry(TimeSpan timeout, TimeSpan delay, int? maxAttempts = null, bool isLinear = false, TimeSpan? maxDelay = null)
            : base (timeout, maxAttempts)
        {
            _delay = delay;
            _isLinear = isLinear;
            _maxDelay = maxDelay;
        }

        protected override TimeSpan? GetDelay(int attempt, TimeSpan timeTaken)
        {
            if (timeTaken > Timeout) return null;

            double delayMilliseconds = 0;
            if (_isLinear) {
                // multiplied backoff
                delayMilliseconds = _delay.TotalMilliseconds*attempt;
            } else if (attempt > 0) {
                // exponential backoff
                // from: http://alexandrebrisebois.wordpress.com/2013/02/19/calculating-an-exponential-back-off-delay-based-on-failed-attempts/
                delayMilliseconds = _delay.TotalMilliseconds / 2d * (Math.Pow(2d, attempt) - 1d);
            }

            if (_maxDelay.HasValue) {
                delayMilliseconds = Math.Min(delayMilliseconds, _maxDelay.Value.TotalMilliseconds);
            }

            return TimeSpan.FromMilliseconds(Math.Min(delayMilliseconds, Timeout.TotalMilliseconds - timeTaken.TotalMilliseconds));
        }
    }
}