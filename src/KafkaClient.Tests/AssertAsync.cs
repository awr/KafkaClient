using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Threading.Tasks;
using AgileObjects.ReadableExpressions;
using Xunit;

namespace KafkaClient.Tests
{
    public static class AssertAsync
    {
        public static Task<bool> ThatEventually(Expression<Func<bool>> predicate, Func<string> messageFunc)
        {
            return ThatEventually(predicate, null, messageFunc);
        }

        public static async Task<bool> ThatEventually(Expression<Func<bool>> predicate, TimeSpan? timeout = null, Func<string> messageFunc = null)
        {
            var compiled = predicate.Compile();
            var timer = Stopwatch.StartNew();
            var timeoutMilliseconds = timeout?.TotalMilliseconds ?? 3000;
            while (compiled() == false) {
                if (timer.ElapsedMilliseconds > timeoutMilliseconds) {
                    if (messageFunc != null) {
                        Assert.True(false, predicate.ToReadableString() + $"\n{messageFunc()}");
                    }
                    Assert.True(false, predicate.ToReadableString());
                }
                await Task.Delay(50).ConfigureAwait(false);
            }
            return true;
        }

        public static async Task Throws<T>(Func<Task> asyncAction, Func<T, bool> when = null) where T : Exception
        {
            try {
                await asyncAction();
                Assert.True(false, $"Should have thrown {typeof(T)}");
            } catch (T ex) {
                if (when != null && !when(ex)) throw;
            }
        }
    }
}