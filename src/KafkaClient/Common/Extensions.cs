using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    public static class Extensions
    {
        #region Log

        /// <summary>
        /// Record verbose information to the log.
        /// </summary>
        public static void Verbose(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Verbose, producer);
        }

        /// <summary>
        /// Record debug information to the log.
        /// </summary>
        public static void Debug(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Debug, producer);
        }

        /// <summary>
        /// Record information to the log.
        /// </summary>
        public static void Info(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Info, producer);
        }

        /// <summary>
        /// Record warning information to the log.
        /// </summary>
        public static void Warn(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Warn, producer);
        }

        /// <summary>
        /// Record error information to the log.
        /// </summary>
        public static void Error(this ILog log, LogEvent logEvent)
        {
            log.Write(LogLevel.Error, () => logEvent);
        }

        #endregion

        #region ArraySegment

        public static bool HasEqualElementsInOrder(this ArraySegment<byte> self, ArraySegment<byte> other)
        {
            if (self.Count != other.Count) return false;
            if (self.Count == 0) return true;

            return self.Zip(other, (s, o) => Equals(s, o)).All(_ => _);
        }

        public static ArraySegment<T> Skip<T>(this ArraySegment<T> self, int offset)
        {
            return new ArraySegment<T>(self.Array, self.Offset + offset, self.Count - offset);
        }
        
        public static short ToInt16(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt16(value.Array, value.Offset).ToBigEndian();
        }

        public static int ToInt32(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt32(value.Array, value.Offset).ToBigEndian();
        }

        public static long ToInt64(this ArraySegment<byte> value)
        {
            return BitConverter.ToInt64(value.Array, value.Offset).ToBigEndian();
        }

        //public static ushort ToUInt16(this ArraySegment<byte> value)
        //{
        //    return BitConverter.ToUInt16(value.Array, value.Offset).ToBigEndian();
        //}

        public static uint ToUInt32(this ArraySegment<byte> value)
        {
            return BitConverter.ToUInt32(value.Array, value.Offset).ToBigEndian();
        }

        //public static ulong ToUInt64(this ArraySegment<byte> value)
        //{
        //    return BitConverter.ToUInt64(value.Array, value.Offset).ToBigEndian();
        //}

        #endregion

        #region Byte

        public static byte[] ToBytes(this short value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this int value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        public static byte[] ToBytes(this long value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        //public static byte[] ToBytes(this ushort value)
        //{
        //    return BitConverter.GetBytes(value.ToBigEndian());
        //}

        public static byte[] ToBytes(this uint value)
        {
            return BitConverter.GetBytes(value.ToBigEndian());
        }

        //public static byte[] ToBytes(this ulong value)
        //{
        //    return BitConverter.GetBytes(value.ToBigEndian());
        //}

        //public static short ToInt16(this byte[] value)
        //{
        //    return BitConverter.ToInt16(value, 0).ToBigEndian();
        //}

        public static int ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(value, 0).ToBigEndian();
        }

        //public static long ToInt64(this byte[] value)
        //{
        //    return BitConverter.ToInt64(value, 0).ToBigEndian();
        //}

        //public static ushort ToUInt16(this byte[] value)
        //{
        //    return BitConverter.ToUInt16(value, 0).ToBigEndian();
        //}

        //public static uint ToUInt32(this byte[] value)
        //{
        //    return BitConverter.ToUInt32(value, 0).ToBigEndian();
        //}

        //public static ulong ToUInt64(this byte[] value)
        //{
        //    return BitConverter.ToUInt64(value, 0).ToBigEndian();
        //}

        private static long ToBigEndian(this long value)
        {
            if (!BitConverter.IsLittleEndian) return value;

            var first = (uint)(value >> 32);
            first = ((first << 24) & 0xFF000000) 
                  | ((first <<  8) & 0x00FF0000) 
                  | ((first >>  8) & 0x0000FF00) 
                  | ((first >> 24) & 0x000000FF);
            var second = (uint)value;
            second = ((second << 24) & 0xFF000000) 
                   | ((second <<  8) & 0x00FF0000) 
                   | ((second >>  8) & 0x0000FF00) 
                   | ((second >> 24) & 0x000000FF);

            return ((long) second << 32) | first;
        }

        //public static ulong ToBigEndian(this ulong value)
        //{
        //    return BitConverter.IsLittleEndian
        //        ? (ulong)ToBigEndian((long)value)
        //        : value;
        //}

        public static int ToBigEndian(this int value)
        {
            return BitConverter.IsLittleEndian
                ? (int)ToBigEndian((uint)value)
                : value;
        }

        private static uint ToBigEndian(this uint value)
        {
            return BitConverter.IsLittleEndian
                ? ((value << 24) & 0xFF000000) 
                | ((value <<  8) & 0x00FF0000) 
                | ((value >>  8) & 0x0000FF00) 
                | ((value >> 24) & 0x000000FF)
                : value;
        }

        private static short ToBigEndian(this short value)
        {
            return BitConverter.IsLittleEndian
                ? (short)(((value & 0xFF) << 8) | ((value >> 8) & 0xFF))
                : value;
        }

        //public static ushort ToBigEndian(this ushort value)
        //{
        //    return BitConverter.IsLittleEndian
        //        ? (ushort)ToBigEndian((short)value)
        //        : value;
        //}

        #endregion

        #region Enumerable / Immutable Collections

        public static IImmutableList<T> AddNotNull<T>(this IImmutableList<T> list, T item) where T : class
        {
            return item != null ? list.Add(item) : list;
        }

        public static IImmutableList<T> AddNotNullRange<T>(this IImmutableList<T> list, IEnumerable<T> items)
        {
            if (items == null) return list;
            if (ReferenceEquals(list, ImmutableList<T>.Empty)) return items.ToImmutableList();
            return list.AddRange(items);
        }

        public static IImmutableList<T> ToSafeImmutableList<T>(this IEnumerable<T> items)
        {
            if (items == null) return ImmutableList<T>.Empty;
            return items.ToImmutableList();
        }

        public static IImmutableDictionary<T, TValue> ToSafeImmutableDictionary<T, TValue>(this IEnumerable<KeyValuePair<T, TValue>> items)
        {
            if (items == null) return ImmutableDictionary<T, TValue>.Empty;
            return items.ToImmutableDictionary();
        }

        public static bool HasEqualElementsInOrder<T>(this IReadOnlyCollection<T> self, IReadOnlyCollection<T> other)
        {
            if (ReferenceEquals(self, other)) return true;
            if (ReferenceEquals(null, other)) return false;
            if (self.Count != other.Count) return false;

            return self.Zip(other, (s, o) => Equals(s, o)).All(_ => _);
        }

        public static string ToStrings<T>(this IEnumerable<T> values)
        {
            return string.Join(",", values.Select(value => value.ToString()));
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer();
            }
        }

        #endregion

        #region Retry

        public static Task<T> TryAsync<T>(
            this IRetry policy,
            Func<int, TimeSpan, Task<RetryAttempt<T>>> func,
            Action<Exception, int, TimeSpan?> onRetry,
            CancellationToken cancellationToken)
        {
            return policy.TryAsync(func, onRetry, null, cancellationToken);
        }

        public static async Task<T> TryAsync<T>(
            this IRetry policy, 
            Func<int, TimeSpan, Task<RetryAttempt<T>>> func, 
            Action<Exception, int, TimeSpan?> onRetry, 
            Action onFailure, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var retryCount = 0;; retryCount++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    var attempt = await func(retryCount, timer.Elapsed).ConfigureAwait(false);
                    if (attempt.IsSuccessful) return attempt.Value;

                    var retryDelay = policy.RetryDelay(retryCount, timer.Elapsed);
                    onRetry?.Invoke(null, retryCount, retryDelay);
                    if (attempt.ShouldRetry && retryDelay.HasValue) {
                        await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                    } else {
                        onFailure?.Invoke();
                        return attempt.Value;
                    }
                } catch (Exception ex) {
                    var retryDelay = policy.RetryDelay(retryCount, timer.Elapsed);
                    onRetry?.Invoke(ex, retryCount, retryDelay);
                    if (retryDelay.HasValue) {
                        await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                    } else {
                        onFailure?.Invoke();
                        throw ex.PrepareForRethrow();
                    }
                }
            }
        }

        #endregion

        #region Exceptions

        /// <summary>
        /// Attempts to prepare the exception for re-throwing by preserving the stack trace. The returned exception should be immediately thrown.
        /// </summary>
        /// <returns>The <see cref="Exception"/> that was passed into this method.</returns>
        public static Exception PrepareForRethrow(this Exception exception)
        {
            if (exception != null) {
                ExceptionDispatchInfo.Capture(exception).Throw();
            }
            return exception;
        }

        public static Exception FlattenAggregates(this IEnumerable<Exception> exceptions)
        {
            var exceptionList = exceptions.ToArray();
            if (exceptionList.Length == 1) return exceptionList[0];

            return new AggregateException(exceptionList.SelectMany<Exception, Exception>(
                ex => {
                    var aggregateException = ex as AggregateException;
                    if (aggregateException != null) return aggregateException.InnerExceptions;
                    return new[] { ex };
                }));
        }

        #endregion

        #region Tasks

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task<T> ThrowIfCancellationRequested<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
            return await task.ConfigureAwait(false);
        }

        public static async Task ThrowIfCancellationRequested(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs))
            {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false))
                {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
            await task.ConfigureAwait(false);
        }

        #endregion

        #region Semaphore

        public static void Lock(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        {
            try {
                semaphore.Wait(cancellationToken);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                action();
            } finally {
                semaphore.Release(1);
            }
        }

        public static T Lock<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        {
            try {
                semaphore.Wait(cancellationToken);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                return function();
            } finally {
                semaphore.Release(1);
            }
        }

        //public static async Task LockAsync(this SemaphoreSlim semaphore, Action action, CancellationToken cancellationToken)
        //{
        //    try {
        //        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        //    } catch (ArgumentNullException ex) {
        //        throw new ObjectDisposedException(nameof(semaphore), ex);
        //    }
        //    try {
        //        action();
        //    } finally {
        //        semaphore.Release(1);
        //    }
        //}

        //public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<T> function, CancellationToken cancellationToken)
        //{
        //    try {
        //        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        //    } catch (ArgumentNullException ex) {
        //        throw new ObjectDisposedException(nameof(semaphore), ex);
        //    }
        //    try {
        //        return function();
        //    } finally {
        //        semaphore.Release(1);
        //    }
        //}

        public static async Task LockAsync(this SemaphoreSlim semaphore, Func<Task> asyncAction, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                await asyncAction();
            } finally {
                semaphore.Release(1);
            }
        }

        public static async Task<T> LockAsync<T>(this SemaphoreSlim semaphore, Func<Task<T>> asyncFunction, CancellationToken cancellationToken)
        {
            try {
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            } catch (ArgumentNullException ex) {
                throw new ObjectDisposedException(nameof(semaphore), ex);
            }
            try {
                return await asyncFunction();
            } finally {
                semaphore.Release(1);
            }
        }

        #endregion
    }
}