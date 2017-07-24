using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class RetryTests
    {
        [Test]
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
            Assert.True(timer.ElapsedMilliseconds < 1000);
        }

        [Test]
        public void RetryNoneDoesNotRetry()
        {
            Assert.Null(Retry.None.RetryDelay(0, TimeSpan.Zero));
        }

        [Test]
        public void RetryAtMostRetriesWithNoDelay()
        {
            Assert.AreEqual(TimeSpan.Zero, Retry.AtMost(1).RetryDelay(0, TimeSpan.Zero));
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(2)]
        [TestCase(5)]
        [TestCase(10)]
        public void RetryAtMostRespectsMaximumAttempts(int maxAttempts)
        {
            var retry = Retry.AtMost(maxAttempts);
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.AreEqual(TimeSpan.Zero, retry.RetryDelay(attempt, TimeSpan.FromHours(1)));
            }
            Assert.Null(retry.RetryDelay(maxAttempts, TimeSpan.FromHours(1)));
            Assert.Null(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromHours(1)));
        }

        [Test]
        public void RetryUntilRetriesWithNoDelay()
        {
            Assert.AreEqual(TimeSpan.Zero, Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero));
        }

        [Test]
        public void RetryUntilRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.Until(maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds >= minDelay.TotalMilliseconds);
                Assert.AreEqual(10 * minDelay.TotalMilliseconds, Retry.Until(maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds);
            }
        }

        [Test]
        public void RetryUntilRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.Until(maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds <= maxDelay.TotalMilliseconds);
                Assert.True(Retry.Until(maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds <= maxDelay.TotalMilliseconds);
            }
        }


        [Test]
        public void RetryUntilRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum);
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.Zero));
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
                Assert.Null(retry.RetryDelay(10, maximum));
                Assert.Null(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)));
            }
        }
        
        [Test]
        public void RetryUntilReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.Null(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
            }
        }

        [Test]
        public void RetryWithBackoffRetriesWithNoDelay()
        {
            Assert.AreEqual(TimeSpan.Zero, Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero));
        }

        [Test]
        public void RetryWithBackoffRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.WithBackoff(100, maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds >= minDelay.TotalMilliseconds);
                Assert.AreEqual(10 * minDelay.TotalMilliseconds, Retry.WithBackoff(100, maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds);
            }
        }

        [Test]
        public void RetryWithBackoffRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.True(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds <= maxDelay.TotalMilliseconds);
                Assert.True(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds <= maxDelay.TotalMilliseconds);
            }
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(2)]
        [TestCase(5)]
        [TestCase(10)]
        public void RetryWithBackoffRespectsMaximumAttempts(int maxAttempts)
        {
            var retry = Retry.WithBackoff(maxAttempts, TimeSpan.FromMinutes(1));
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.NotNull(retry.RetryDelay(attempt, TimeSpan.FromSeconds(1)));
            }
            Assert.Null(retry.RetryDelay(maxAttempts, TimeSpan.FromSeconds(1)));
            Assert.Null(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromSeconds(1)));
        }

        [Test]
        public void RetryWithBackoffRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum);
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.Zero));
                Assert.NotNull(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
                Assert.Null(retry.RetryDelay(10, maximum));
                Assert.Null(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)));
            }
        }

        [Test]
        public void RetryWithBackoffReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.Null(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)));
            }
        }

        [Test]
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

        [Test]
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