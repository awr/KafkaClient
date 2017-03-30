using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    public class RetryTests
    {
        [Fact]
        public async Task NoDelayBeforeFirstAttempt()
        {
            var timer = new Stopwatch();
            timer.Start();
            await Retry.WithBackoff(5, minimumDelay: TimeSpan.FromSeconds(1), maximumDelay: TimeSpan.FromSeconds(1))
                       .TryAsync(
                           (retryAttempt, elapsed) => {
                               timer.Stop();
                               return Task.FromResult(new RetryAttempt<long>(timer.ElapsedMilliseconds));
                           },
                           (ex, retryAttempt, retryDelay) => { },
                           CancellationToken.None);
            Assert.That(timer.ElapsedMilliseconds, Is.LessThan(1000));
        }

        [Fact]
        public void RetryNoneDoesNotRetry()
        {
            Assert.That(Retry.None.RetryDelay(0, TimeSpan.Zero), Is.Null);
        }

        [Fact]
        public void RetryAtMostRetriesWithNoDelay()
        {
            Assert.Equal(Retry.AtMost(1).RetryDelay(0, TimeSpan.Zero), TimeSpan.Zero);
        }

        [Fact]
        public void RetryAtMostRespectsMaximumAttempts([Range(0, 10)] int maxAttempts)
        {
            var retry = Retry.AtMost(maxAttempts);
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.Equal(retry.RetryDelay(attempt, TimeSpan.FromHours(1)), TimeSpan.Zero);
            }
            Assert.That(retry.RetryDelay(maxAttempts, TimeSpan.FromHours(1)), Is.Null);
            Assert.That(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromHours(1)), Is.Null);
        }

        [Fact]
        public void RetryUntilRetriesWithNoDelay()
        {
            Assert.Equal(Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero), TimeSpan.Zero);
        }

        [Fact]
        public void RetryUntilRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.Until(maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds >= minDelay.TotalMilliseconds);
                Assert.Equal(Retry.Until(maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, 10 * minDelay.TotalMilliseconds);
            }
        }

        [Fact]
        public void RetryUntilRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.Until(maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
                Assert.That(Retry.Until(maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
            }
        }


        [Fact]
        public void RetryUntilRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum);
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.Zero));
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
                Assert.That(retry.RetryDelay(10, maximum), Is.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)), Is.Null);
            }
        }
        
        [Fact]
        public void RetryUntilReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Null);
            }
        }

        [Fact]
        public void RetryWithBackoffRetriesWithNoDelay()
        {
            Assert.Equal(Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero), TimeSpan.Zero);
        }

        [Fact]
        public void RetryWithBackoffRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.WithBackoff(100, maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds >= minDelay.TotalMilliseconds);
                Assert.Equal(Retry.WithBackoff(100, maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, 10 * minDelay.TotalMilliseconds);
            }
        }

        [Fact]
        public void RetryWithBackoffRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
                Assert.That(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
            }
        }

        [Fact]
        public void RetryWithBackoffRespectsMaximumAttempts([Range(0, 10)] int maxAttempts)
        {
            var retry = Retry.WithBackoff(maxAttempts, TimeSpan.FromMinutes(1));
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.NotNull(retry.RetryDelay(attempt, TimeSpan.FromSeconds(1)));
            }
            Assert.That(retry.RetryDelay(maxAttempts, TimeSpan.FromSeconds(1)), Is.Null);
            Assert.That(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromSeconds(1)), Is.Null);
        }

        [Fact]
        public void RetryWithBackoffRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum);
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.Zero));
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
                Assert.That(retry.RetryDelay(10, maximum), Is.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)), Is.Null);
            }
        }

        [Fact]
        public void RetryWithBackoffReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Null);
            }
        }

        [Fact]
        public void RetryWithBackoffHasIncreasingDelay()
        {
            foreach (var maximum in new[] { TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum, TimeSpan.FromTicks(maximum.Ticks / 100));
                TimeSpan lastDelay = TimeSpan.Zero;
                for (var attempt = 0; attempt < 10; attempt++) {
                    var delay = retry.RetryDelay(attempt, lastDelay);
                    Assert.NotNull(delay);
                    Assert.True(delay.Value.TotalMilliseconds >= lastDelay.TotalMilliseconds);
                    lastDelay = delay.Value;
                }
            }
        }

        [Fact]
        public void RetryUntilHasIncreasingDelay()
        {
            foreach (var maximum in new[] { TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum, TimeSpan.FromTicks(maximum.Ticks / 100));
                TimeSpan lastDelay = TimeSpan.Zero;
                for (var attempt = 0; attempt < 10; attempt++) {
                    var delay = retry.RetryDelay(attempt, lastDelay);
                    Assert.NotNull(delay);
                    Assert.True(delay.Value.TotalMilliseconds >= lastDelay.TotalMilliseconds);
                    lastDelay = delay.Value;
                }
            }
        }
    }
}