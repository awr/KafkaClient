using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class Extensions
    {
        /// <summary>
        /// Splits a collection into given batch sizes and returns as an enumerable of batches.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> collection, int batchSize)
        {
            var nextbatch = new List<T>(batchSize);
            foreach (T item in collection)
            {
                nextbatch.Add(item);
                if (nextbatch.Count == batchSize)
                {
                    yield return nextbatch;
                    nextbatch = new List<T>(batchSize);
                }
            }
            if (nextbatch.Count > 0)
                yield return nextbatch;
        }

        public static async Task TemporaryTopicAsync(this IRouter router, Func<string, Task> asyncAction, int partitions = 1, [CallerMemberName] string name = null)
        {
            var topicName = TestConfig.TopicName(name);
            try {
                await router.SendToAnyAsync(new CreateTopicsRequest(new [] { new CreateTopicsRequest.Topic(topicName, partitions, 1) }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            } catch (RequestException ex) when (ex.ErrorCode == ErrorCode.TOPIC_ALREADY_EXISTS) {
                // ignore already exists
            }
            try {
                await asyncAction(topicName);
            } finally {
                // right now deleting the topic isn't propagating properly, so subsequent runs of the test fail
                // await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromSeconds(1)), CancellationToken.None);
            }
        }

        public static async Task DeleteTopicAsync(this IRouter router, [CallerMemberName] string name = null)
        {
            var topicName = TestConfig.TopicName(name);
            try {
                var response = await router.SendToAnyAsync(new DeleteTopicsRequest(new [] { topicName }, TimeSpan.FromMilliseconds(500)), CancellationToken.None);
                if (response.Errors.Any(e => e == ErrorCode.REQUEST_TIMED_OUT)) {
                    Assert.Inconclusive("Cannot validate when topic remains");
                }
            } catch (RequestException ex) when (ex.ErrorCode == ErrorCode.TOPIC_ALREADY_EXISTS) {
                // ignore already exists
            }
        }

        public static async Task CommitTopicOffsetAsync(this IRouter router, string topicName, int partitionId, string groupId, long offset, CancellationToken cancellationToken)
        {
            var request = new OffsetCommitRequest(groupId, new [] { new OffsetCommitRequest.Topic(topicName, partitionId, offset) });
            await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
        }

        public static byte[] ToBytes(this string value)
        {
            if (String.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToIntSizedBytes(this string value)
        {
            if (String.IsNullOrEmpty(value)) return (-1).ToBytes();

            return value.Length.ToBytes()
                         .Concat(value.ToBytes())
                         .ToArray();
        }

        public static string ToUtf8String(this ArraySegment<byte> value)
        {
            if (value.Count == 0) return String.Empty;

            return Encoding.UTF8.GetString(value.Array, value.Offset, value.Count);
        }

        public static Task<T> CancelAfter<T>(this Task<T> task, int timeoutMilliseconds = 3000)
        {
            return CancelAfter(task, TimeSpan.FromMilliseconds(timeoutMilliseconds));
        }

        public static async Task<T> CancelAfter<T>(this Task<T> task, TimeSpan timeout)
        {
            var delay = Task.Delay(timeout);
            if (task != await Task.WhenAny(task, delay).ConfigureAwait(false)) {
                throw new OperationCanceledException();
            }
            return await task.ConfigureAwait(false);
        }
    }
}