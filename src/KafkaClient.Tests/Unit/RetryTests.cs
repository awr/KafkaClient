using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
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
            Assert.That(timer.ElapsedMilliseconds, Is.LessThan(1000));
        }

        [Test]
        public void RetryNoneDoesNotRetry()
        {
            Assert.That(Retry.None.RetryDelay(0, TimeSpan.Zero), Is.Null);
        }

        [Test]
        public void RetryAtMostRetriesWithNoDelay()
        {
            Assert.That(Retry.AtMost(1).RetryDelay(0, TimeSpan.Zero), Is.EqualTo(TimeSpan.Zero));
        }

        [Test]
        public void RetryAtMostRespectsMaximumAttempts([Range(0, 10)] int maxAttempts)
        {
            var retry = Retry.AtMost(maxAttempts);
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.That(retry.RetryDelay(attempt, TimeSpan.FromHours(1)), Is.EqualTo(TimeSpan.Zero));
            }
            Assert.That(retry.RetryDelay(maxAttempts, TimeSpan.FromHours(1)), Is.Null);
            Assert.That(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromHours(1)), Is.Null);
        }

        [Test]
        public void RetryUntilRetriesWithNoDelay()
        {
            Assert.That(Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero), Is.EqualTo(TimeSpan.Zero));
        }

        [Test]
        public void RetryUntilRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.Until(maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtLeast(minDelay.TotalMilliseconds));
                Assert.That(Retry.Until(maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.EqualTo(10 * minDelay.TotalMilliseconds));
            }
        }

        [Test]
        public void RetryUntilRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.Until(maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
                Assert.That(Retry.Until(maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
            }
        }


        [Test]
        public void RetryUntilRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum);
                Assert.That(retry.RetryDelay(10, TimeSpan.Zero), Is.Not.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Not.Null);
                Assert.That(retry.RetryDelay(10, maximum), Is.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)), Is.Null);
            }
        }
        
        [Test]
        public void RetryUntilReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.Until(maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Null);
            }
        }

        [Test]
        public void RetryWithBackoffRetriesWithNoDelay()
        {
            Assert.That(Retry.Until(TimeSpan.FromMinutes(1)).RetryDelay(10, TimeSpan.Zero), Is.EqualTo(TimeSpan.Zero));
        }

        [Test]
        public void RetryWithBackoffRespectsMinDelay()
        {
            foreach (var minDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.WithBackoff(100, maximum, minDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtLeast(minDelay.TotalMilliseconds));
                Assert.That(Retry.WithBackoff(100, maximum, minDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.EqualTo(10 * minDelay.TotalMilliseconds));
            }
        }

        [Test]
        public void RetryWithBackoffRespectsMaxDelay()
        {
            foreach (var maxDelay in new [] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(105) }) {
                var maximum = TimeSpan.FromMinutes(1);
                Assert.That(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(0, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
                Assert.That(Retry.WithBackoff(100, maximum, maximum, maxDelay).RetryDelay(9, TimeSpan.Zero).GetValueOrDefault().TotalMilliseconds, Is.AtMost(maxDelay.TotalMilliseconds));
            }
        }

        [Test]
        public void RetryWithBackoffRespectsMaximumAttempts([Range(0, 10)] int maxAttempts)
        {
            var retry = Retry.WithBackoff(maxAttempts, TimeSpan.FromMinutes(1));
            for (var attempt = 0; attempt < maxAttempts; attempt++) {
                Assert.That(retry.RetryDelay(attempt, TimeSpan.FromSeconds(1)), Is.Not.Null);
            }
            Assert.That(retry.RetryDelay(maxAttempts, TimeSpan.FromSeconds(1)), Is.Null);
            Assert.That(retry.RetryDelay(maxAttempts + 1, TimeSpan.FromSeconds(1)), Is.Null);
        }

        [Test]
        public void RetryWithBackoffRespectsMaximumTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum);
                Assert.That(retry.RetryDelay(10, TimeSpan.Zero), Is.Not.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Not.Null);
                Assert.That(retry.RetryDelay(10, maximum), Is.Null);
                Assert.That(retry.RetryDelay(10, TimeSpan.FromMilliseconds(maximum.TotalMilliseconds * 1.01)), Is.Null);
            }
        }

        [Test]
        public void RetryWithBackoffReturnsEarlyIfDelayIsPastMaxTime()
        {
            foreach (var maximum in new[] { TimeSpan.FromMilliseconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), TimeSpan.FromHours(1), TimeSpan.FromDays(1) }) {
                var retry = Retry.WithBackoff(100, maximum, TimeSpan.FromTicks(maximum.Ticks / 2));
                Assert.That(retry.RetryDelay(10, TimeSpan.FromTicks(maximum.Ticks / 2)), Is.Null);
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
                    Assert.That(delay, Is.Not.Null);
                    Assert.That(delay.Value.TotalMilliseconds, Is.AtLeast(lastDelay.TotalMilliseconds));
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
                    Assert.That(delay, Is.Not.Null);
                    Assert.That(delay.Value.TotalMilliseconds, Is.AtLeast(lastDelay.TotalMilliseconds));
                    lastDelay = delay.Value;
                }
            }
        }
    }
}