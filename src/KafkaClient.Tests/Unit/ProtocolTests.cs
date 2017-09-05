using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class ProtocolTests
    {
        private readonly Random _randomizer = new Random();

        [Test]
        public void HeaderShouldCorrectPackByteLengths()
        {
            var result = new ApiVersionsRequest().ToBytes(new RequestContext(123456789, clientId: "test"));

            var withoutLength = new byte[result.Count - 4];
            Buffer.BlockCopy(result.Array, 4, withoutLength, 0, result.Count - 4);
            Assert.AreEqual(14, withoutLength.Length);
            Assert.AreEqual(new byte[] { 0, 18, 0, 0, 7, 91, 205, 21, 0, 4, 116, 101, 115, 116 }, withoutLength);
        }

        #region Messages

        [Test]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var message = new Message(value: "kafka test message.", key: "test");

            using (var writer = new KafkaWriter()) {
                writer.Write(0L);
                using (writer.MarkForLength()) {
                    writer.WriteMessage(message, new MessageContext(), out int _);
                }
                var encoded = writer.ToSegment(true);
                encoded.Array[encoded.Offset + 20] += 1;
                using (var reader = new KafkaReader(encoded)) {
                    Assert.Throws<CrcValidationException>(() => reader.ReadMessages());
                }
            }
        }

        [Test]
        public void NullMessageIsNotEqualToNotNullMessage()
        {
            var m = new Message(null);
            Assert.AreNotEqual(m, null);
        }

        [Test]
        public void NullAndEmptyStringMessageAreEqual()
        {
            var m1 = new Message(null);
            var m2 = new Message("");
            Assert.AreEqual(m1.GetHashCode(), m2.GetHashCode());
            Assert.AreEqual(m1, m2);
        }
        
        [Test]
        public void EnsureMessageSetEncodeAndDecodeAreCompatible(
            [Values(0, 1, 2)] byte version,
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec,
            [Values(1, 10)] int valueLength,
            [Values(0, 1, 10)] int keyLength)
        {
            var random = new Random(42);
            var messages = new List<Message>();
            for (var m = 0; m < 3; m++) {
                var key = keyLength > 0 ? new byte[keyLength] : null;
                var value = new byte[valueLength];
                if (key != null) {
                    random.NextBytes(key);
                }
                random.NextBytes(value);

                var offset = codec == MessageCodec.None ? m*2 : m;
                var keyBytes = key != null ? new ArraySegment<byte>(key) : new ArraySegment<byte>();
                var dateTimeOffset = version > 0 ? DateTimeOffset.UtcNow.AddMilliseconds(m) : (DateTimeOffset?)null;

                var headers = new List<MessageHeader>();
                if (version > 0) {
                    for (var h = 0; h < keyLength; h++) {
                        var headerValue = new byte[valueLength];
                        random.NextBytes(headerValue);
                        headers.Add(new MessageHeader(h.ToString(), new ArraySegment<byte>(headerValue)));
                    }
                }
                messages.Add(new Message(new ArraySegment<byte>(value), keyBytes, 0, offset, dateTimeOffset));
            }

            using (var writer = new KafkaWriter()) {
                writer.WriteMessages(messages, new TransactionContext(), version, codec, out int _);
                var encoded = writer.ToSegment(false);
                using (var reader = new KafkaReader(encoded)) {
                    var result = reader.ReadMessages(encoded.Count).Messages;

                    for (var i = 0; i < messages.Count; i++) {
                        if (!messages[i].Equals(result[i])) {
                            Assert.AreEqual(messages[i].ToVerboseString(), result[i].ToVerboseString());
                            Assert.AreEqual(messages[i], result[i]);
                        }
                    }
                }
            }
        }

        [Test]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[] {
                new Message("0", "1"),
                new Message("1", "1"),
                new Message("2", "1")
            };

            using (var writer = new KafkaWriter())
            {
                writer.WriteMessages(messages, new TransactionContext(), 0, MessageCodec.None, out int _);
                var result = writer.ToSegment(false);
                Assert.AreEqual(expected, result);
            }
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            using (var reader = new KafkaReader(MessageHelper.FetchResponseMaxBytesOverflow)) {
                //This message set has a truncated message bytes at the end of it
                var result = reader.ReadMessages();

                var message = result.Messages.First().Value.ToUtf8String();

                Assert.AreEqual("test", message);
                Assert.AreEqual(529, result.Messages.Count);
            }
        }

        [Test]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            // arrange
            var message = new byte[] { };
            var messageSize = message.Length + 1;
            using (var writer = new KafkaWriter())
            {
                writer.Write(0L)
                       .Write(messageSize)
                       .Write(new ArraySegment<byte>(message));
                var segment = writer.ToSegment(false);
                using (var reader = new KafkaReader(segment))
                {
                    // act/assert
                    Assert.Throws<BufferUnderRunException>(() => reader.ReadMessages(segment.Count));
                }
            }
        }

        [Test]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new ArraySegment<byte>(new byte[] { 1, 2, 3, 4 });
            using (var writer = new KafkaWriter())
            {
                writer.Write(0L);
                using (writer.MarkForLength())
                {
                    writer.WriteMessage(new Message(expectedPayloadBytes, new ArraySegment<byte>(new byte[] { 0 }), 0), new MessageContext(), out int _);
                }
                var segment = writer.ToSegment(false);

                // act/assert
                using (var reader = new KafkaReader(segment)) {
                    var batch = reader.ReadMessages(segment.Count);
                    var messages = batch.Messages;
                    var actualPayload = messages.First().Value;

                    // assert
                    var expectedPayload = new byte[] { 1, 2, 3, 4 };
                    Assert.True(actualPayload.HasEqualElementsInOrder(expectedPayload));
                }
            }
        }

        [Test]
        public void MessageEquality(
            [Values(0, 1, 2)] byte version,
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec,
            [Values(1, 10)] int valueLength,
            [Values(0, 1, 10)] int keyLength)
        {
            var random = new Random(42);
            var key = keyLength > 0 ? new byte[keyLength] : null;
            var value = new byte[valueLength];
            if (key != null) {
                random.NextBytes(key);
            }
            random.NextBytes(value);

            var message = new Message(new ArraySegment<byte>(value), key != null ? new ArraySegment<byte>(key) : new ArraySegment<byte>(), (byte)codec, timestamp: version > 0 ? DateTimeOffset.UtcNow : (DateTimeOffset?)null);
            message.AssertEqualToSelf();
            message.AssertNotEqual(new Message[] { null });
            message.AssertNotEqual(new Message(new ArraySegment<byte>(value, 1, valueLength - 1), message.Key, message.Attribute, message.Offset, message.Timestamp));
            if (keyLength > 0) {
                message.AssertNotEqual(new Message(message.Value, new ArraySegment<byte>(key, 1, keyLength - 1), message.Attribute, message.Offset, message.Timestamp));
            }
            message.AssertNotEqual(new Message(message.Value, message.Key, (byte)(message.Attribute + 1), message.Offset, message.Timestamp));
            message.AssertNotEqual(new Message(message.Value, message.Key, message.Attribute, message.Offset + 1L, message.Timestamp));
            if (version > 0) {
                message.AssertNotEqual(new Message(message.Value, message.Key, message.Attribute, message.Offset, null));
            }
        }

        #endregion

        #region Request / Response

        [Test]
        public void ProduceRequestSnappy(
            [Values(0, 1, 2, 3)] short version,
            [Values(0, 2, -1)] short acks, 
            [Values(0, 1000)] int timeoutMilliseconds, 
            [Values("testTopic")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(3)] int messagesPerSet)
        {
            ProduceRequest(version, acks, timeoutMilliseconds, topic, topicsPerRequest, totalPartitions, messagesPerSet, MessageCodec.Snappy);
        }

        private byte MessageVersion(short version)
        {
            switch (version) {
                case 5:
                case 4:
                case 3: return (byte)2;
                case 2: return (byte)1;
                default: return (byte) 0;
            }
        }

        [Test]
        public void ProduceRequest(
            [Values(0, 1, 2, 3)] short version,
            [Values(0, 2, -1)] short acks,
            [Values(0, 1000)] int timeoutMilliseconds,
            [Values("testTopic")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int totalPartitions,
            [Values(3)] int messagesPerSet,
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec)
        {
            var payloads = new List<ProduceRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partition = 1 + t % totalPartitions;
                payloads.Add(new ProduceRequest.Topic(topicName + t, partition, GenerateMessages(messagesPerSet, MessageVersion(version), codec), codec));
            }
            var request = new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks, version >= 3 ? topicName : null);
            var requestWithUpdatedAttribute = new ProduceRequest(request.Topics.Select(t => new ProduceRequest.Topic(t.TopicName, t.PartitionId,
                    t.Messages.Select(m => m.Attribute == 0 ? m : new Message(m.Value, m.Key, 0, m.Offset, m.Timestamp)))),
                request.Timeout, request.Acks, request.TransactionalId);

            request.AssertCanEncodeDecodeRequest(version, forComparison: requestWithUpdatedAttribute);
        }

        [Test]
        public void ProduceRequestEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values(-1)] short acks,
            [Values(1000)] int timeoutMilliseconds,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int totalPartitions,
            [Values(3)] int messagesPerSet,
            [Values(MessageCodec.Gzip)] MessageCodec codec)
        {
            var topics = new List<ProduceRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partition = 1 + t % totalPartitions;
                topics.Add(new ProduceRequest.Topic(topicName + t, partition, GenerateMessages(messagesPerSet, MessageVersion(version), codec), codec));
            }
            var alternate = topics.Take(1).Select(p => new ProduceRequest.Topic(topicName, p.PartitionId, p.Messages, p.Codec));
            var request = new ProduceRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks, version >= 3 ? topicName : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new ProduceRequest(alternate.Union(topics.Skip(1)), TimeSpan.FromMilliseconds(timeoutMilliseconds), acks, request.TransactionalId),
                new ProduceRequest(topics.Skip(1), TimeSpan.FromMilliseconds(timeoutMilliseconds + 1), acks, request.TransactionalId),
                new ProduceRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds + 1), acks, request.TransactionalId),
                new ProduceRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), (short)(acks + 1), request.TransactionalId)
                );
            if (version >= 3) {
                request.AssertNotEqual(new ProduceRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks, request.TransactionalId + 1));
            }
        }

        [Test]
        public void ProduceRequestTopicEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(5)] int totalPartitions,
            [Values(3)] int messagesPerSet,
            [Values(MessageCodec.Gzip)] MessageCodec codec)
        {
            var topic = new ProduceRequest.Topic(
                topicName, totalPartitions, GenerateMessages(messagesPerSet, MessageVersion(version), codec), codec);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new ProduceRequest.Topic(topicName + 1, totalPartitions, GenerateMessages(messagesPerSet, MessageVersion(version), codec), codec),
                new ProduceRequest.Topic(topicName, totalPartitions + 1, GenerateMessages(messagesPerSet, MessageVersion(version), codec), codec),
                new ProduceRequest.Topic(topicName, totalPartitions, GenerateMessages(messagesPerSet + 1, MessageVersion(version), codec), codec));
        }

        [Test]
        public void ProduceResponse(
            [Values(0, 1, 2, 3)] short version,
            [Values(-1, 0, 10000000)] long timestampMilliseconds, 
            [Values("testTopic")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorCode.NONE,
                ErrorCode.CORRUPT_MESSAGE
            )] ErrorCode errorCode,
            [Values(0, 100000)] int throttleTime)
        {
            var topics = new List<ProduceResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new ProduceResponse.Topic(topicName + t, t % totalPartitions, errorCode, _randomizer.Next(), version >= 2 ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds) : (DateTimeOffset?)null));
            }
            var response = new ProduceResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void ProduceResponseEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values(10000000)] long timestampMilliseconds, 
            [Values("testTopic")] string topicName, 
            [Values(2)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorCode.NONE,
                ErrorCode.CORRUPT_MESSAGE
            )] ErrorCode errorCode,
            [Values(100000)] int throttleTime)
        {
            var topics = new List<ProduceResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new ProduceResponse.Topic(topicName + t, t % totalPartitions, errorCode, _randomizer.Next(), version >= 2 ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds) : (DateTimeOffset?)null));
            }
            var alternate = topics.Take(1).Select(p => new ProduceResponse.Topic(topicName, p.PartitionId, p.Error, p.BaseOffset, p.Timestamp));
            var response = new ProduceResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new ProduceResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime),
                new ProduceResponse(topics.Skip(1), response.ThrottleTime)
                );
        }

        [Test]
        public void FetchRequest(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values(0, 100)] int maxWaitMilliseconds, 
            [Values(0, 64000)] int minBytes, 
            [Values("testTopic")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var fetches = new List<FetchRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                fetches.Add(new FetchRequest.Topic(topicName + t, t % totalPartitions, _randomizer.Next(0, int.MaxValue), version >= 5 ? (long?)_randomizer.Next(0, int.MaxValue) : null, maxBytes));
            }
            var request = new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitMilliseconds), minBytes, version >= 3 ? maxBytes / _randomizer.Next(1, maxBytes) : 0, version >= 4 ? (byte?)_randomizer.Next(0, 1) : null);
            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void FetchRequestTopicEquality(
            [Values("testTopic")] string topicName, 
            [Values(2)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var topic = new FetchRequest.Topic(topicName, totalPartitions, _randomizer.Next(0, int.MaxValue), _randomizer.Next(0, int.MaxValue), maxBytes);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new FetchRequest.Topic(topicName + 1, totalPartitions, topic.FetchOffset, topic.LogStartOffset, maxBytes),
                new FetchRequest.Topic(topicName, totalPartitions + 1, topic.FetchOffset, topic.LogStartOffset, maxBytes),
                new FetchRequest.Topic(topicName, totalPartitions, topic.FetchOffset + 1, topic.LogStartOffset, maxBytes),
                new FetchRequest.Topic(topicName, totalPartitions, topic.FetchOffset, topic.LogStartOffset + 1, maxBytes),
                new FetchRequest.Topic(topicName, totalPartitions, topic.FetchOffset, topic.LogStartOffset, maxBytes + 1));
        }

        [Test]
        public void FetchRequestEquality(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values(100)] int maxWaitMilliseconds, 
            [Values(64000)] int minBytes, 
            [Values("testTopic")] string topicName, 
            [Values(2)] int topicsPerRequest, 
            [Values(2)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var topics = new List<FetchRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new FetchRequest.Topic(topicName + t, t % totalPartitions, _randomizer.Next(0, int.MaxValue), _randomizer.Next(0, int.MaxValue), maxBytes));
            }
            var alternate = topics.Take(1).Select(p => new FetchRequest.Topic(topicName, p.PartitionId, p.FetchOffset, p.MaxBytes));
            var request = new FetchRequest(topics, TimeSpan.FromMilliseconds(maxWaitMilliseconds), minBytes, version >= 3 ? maxBytes / _randomizer.Next(1, maxBytes) : 0, version >= 4 ? (byte?)_randomizer.Next(0, 1) : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new FetchRequest(alternate.Union(topics.Skip(1)), request.MaxWaitTime, request.MinBytes, request.MaxBytes, request.IsolationLevel),
                new FetchRequest(topics.Skip(1), request.MaxWaitTime, request.MinBytes, request.MaxBytes, request.IsolationLevel),
                new FetchRequest(topics, TimeSpan.FromMilliseconds(maxWaitMilliseconds + 1), request.MinBytes, request.MaxBytes, request.IsolationLevel),
                new FetchRequest(topics, request.MaxWaitTime, request.MinBytes + 1, request.MaxBytes, request.IsolationLevel),
                new FetchRequest(topics, request.MaxWaitTime, request.MinBytes, request.MaxBytes + 1, request.IsolationLevel),
                new FetchRequest(topics, request.MaxWaitTime, request.MinBytes, request.MaxBytes, request.IsolationLevel == 0 ? (byte)1 : (byte)0)
                );
        }

        [Test]
        public void FetchResponse(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("testTopic")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec, 
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_OUT_OF_RANGE
            )] ErrorCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var topics = new List<FetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                var messages = GenerateMessages(messagesPerSet, MessageVersion(version), codec);
                var abortedTransactions = version >= 4
                    ? totalPartitions.Repeat(i => new FetchResponse.AbortedTransaction(i, i)).ToList()
                    : null;
                topics.Add(new FetchResponse.Topic(topicName + t, partitionId, _randomizer.Next(), errorCode, version >= 4 ? (long?)topicsPerRequest : null, version >= 5 ? (long?)topicsPerRequest : null, messages, abortedTransactions));
            }
            var response = new FetchResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);
            var responseWithUpdatedAttribute = new FetchResponse(response.Responses.Select(t => new FetchResponse.Topic(t.TopicName, t.PartitionId, t.HighWatermark, t.Error, t.LastStableOffset, t.LogStartOffset,
                    t.Messages.Select(m => m.Attribute == 0 ? m : new Message(m.Value, m.Key, 0, m.Offset, m.Timestamp)), t.AbortedTransactions)), 
                response.ThrottleTime);

            response.AssertCanEncodeDecodeResponse(version, forComparison: responseWithUpdatedAttribute);
        }

        [Test]
        public void FetchResponseAbortedTransactionEquality([Values(2)] int totalPartitions)
        {
            var abortedTransaction = new FetchResponse.AbortedTransaction(totalPartitions, totalPartitions);
            abortedTransaction.AssertEqualToSelf();
            abortedTransaction.AssertNotEqual(null,
                new FetchResponse.AbortedTransaction(totalPartitions + 1, totalPartitions),
                new FetchResponse.AbortedTransaction(totalPartitions, totalPartitions + 1)
            );
        }

        [Test]
        public void FetchResponseTopicEquality(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values("testTopic")] string topicName, 
            [Values(2)] int totalPartitions, 
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec, 
            [Values(
                ErrorCode.OFFSET_OUT_OF_RANGE
            )] ErrorCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var messages = GenerateMessages(messagesPerSet, MessageVersion(version), codec);
            var abortedTransactions = version >= 4
                ? totalPartitions.Repeat(i => new FetchResponse.AbortedTransaction(i, i)).ToList()
                : null;
            var topic = new FetchResponse.Topic(topicName, totalPartitions, _randomizer.Next(), errorCode, version >= 4 ? (long?)totalPartitions : null, version >= 5 ? (long?)totalPartitions : null, messages, abortedTransactions);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new FetchResponse.Topic(topicName + 1, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages, topic.AbortedTransactions),
                new FetchResponse.Topic(topicName, totalPartitions + 1, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages, topic.AbortedTransactions),
                new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark + 1, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages, topic.AbortedTransactions),
                new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error + 1, topic.LastStableOffset, topic.LogStartOffset, messages, topic.AbortedTransactions),
                new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages.Skip(1), topic.AbortedTransactions),
                new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, ModifyMessages(messages), topic.AbortedTransactions)
                );
            if (version >= 4) {
                topic.AssertNotEqual(new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset.GetValueOrDefault() + 1, topic.LogStartOffset, messages, topic.AbortedTransactions));
                topic.AssertNotEqual(new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages, topic.AbortedTransactions.Skip(1)));
                topic.AssertNotEqual(new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset, messages, (new [] {new FetchResponse.AbortedTransaction(totalPartitions + 1, 0)}).Union(topic.AbortedTransactions.Skip(1))));
            }
            if (version >= 5) {
                topic.AssertNotEqual(new FetchResponse.Topic(topicName, totalPartitions, topic.HighWatermark, topic.Error, topic.LastStableOffset, topic.LogStartOffset.GetValueOrDefault() + 1, messages, topic.AbortedTransactions));
            }
        }

        [Test]
        public void FetchResponseEquality(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("testTopic")] string topicName, 
            [Values(2)] int topicsPerRequest, 
            [Values(2)] int totalPartitions, 
            [Values(MessageCodec.None, MessageCodec.Gzip)] MessageCodec codec, 
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_OUT_OF_RANGE
            )] ErrorCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var topics = new List<FetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                var messages = GenerateMessages(messagesPerSet, MessageVersion(version), codec);
                topics.Add(new FetchResponse.Topic(topicName + t, partitionId, _randomizer.Next(), errorCode, messages: messages));
            }
            var alternate = topics.Take(1).Select(p => new FetchResponse.Topic(topicName, p.PartitionId, p.HighWatermark, p.Error, messages: p.Messages));
            var response = new FetchResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);
            response.AssertNotEqual(null,
                new FetchResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime),
                new FetchResponse(topics.Skip(1), response.ThrottleTime)
                );
        }

        [Test]
        public void FetchResponseSnappy(
            [Values(0, 1, 2, 3, 4, 5)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("testTopic")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int totalPartitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_OUT_OF_RANGE
            )] ErrorCode errorCode,
            [Values(3)] int messagesPerSet
        )
        {
            FetchResponse(version, throttleTime, topicName, topicsPerRequest, totalPartitions, MessageCodec.Snappy, errorCode, messagesPerSet);
        }

        [Test]
        public void OffsetsRequest(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(-2, -1, 123456, 10000000)] long time,
            [Values(1, 10)] int maxOffsets)
        {
            var topics = new List<OffsetsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var offset = new OffsetsRequest.Topic(topicName + t, t % totalPartitions, time, version == 0 ? maxOffsets : 1);
                topics.Add(offset);
            }
            var request = new OffsetsRequest(topics, version >= 2 ? (byte?)IsolationLevels.ReadCommitted : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void OffsetsRequestTopicEquality(
            [Values(0, 1)] short version,
            [Values("testTopic")] string topicName,
            [Values(2)] int totalPartitions,
            [Values(-2, 123456)] long time,
            [Values(3)] int maxOffsets)
        {
            var topic = new OffsetsRequest.Topic(topicName, totalPartitions, time, version == 0 ? maxOffsets : 1);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetsRequest.Topic(topicName + 1, totalPartitions, time, version == 0 ? maxOffsets : 1),
                new OffsetsRequest.Topic(topicName, totalPartitions + 1, time, version == 0 ? maxOffsets : 1),
                new OffsetsRequest.Topic(topicName, totalPartitions, time + 1, version == 0 ? maxOffsets : 1));
            if (version == 0) {
                topic.AssertNotEqual(new OffsetsRequest.Topic(topicName, totalPartitions, time, 2));
            }
        }

        [Test]
        public void OffsetsRequestEquality(
            [Values(0, 1)] short version,
            [Values("testTopic")] string topicName, 
            [Values(3)] int topicsPerRequest, 
            [Values(2)] int totalPartitions, 
            [Values(123456)] long time,
            [Values(2)] int maxOffsets)
        {
            var topics = new List<OffsetsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var offset = new OffsetsRequest.Topic(topicName + t, t % totalPartitions, time, version == 0 ? maxOffsets : 1);
                topics.Add(offset);
            }
            var alternate = topics.Take(1).Select(p => new OffsetsRequest.Topic(topicName, p.PartitionId, p.Timestamp, p.MaxNumOffsets));
            var request = new OffsetsRequest(topics, version >= 2 ? (byte?)IsolationLevels.ReadCommitted : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new OffsetsRequest(alternate.Union(topics.Skip(1)), request.IsolationLevel),
                new OffsetsRequest(topics.Skip(1), request.IsolationLevel)
                );
            if (version >= 2) {
                request.AssertNotEqual(new OffsetsRequest(topics, IsolationLevels.ReadUnCommitted));
            }
        }

        [Test]
        public void OffsetsResponse(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                ErrorCode.NOT_LEADER_FOR_PARTITION,
                ErrorCode.UNKNOWN
            )] ErrorCode errorCode, 
            [Values(1, 5)] int offsetsPerPartition)
        {
            var topics = new List<OffsetsResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                for (var o = 0; o < offsetsPerPartition; o++) {
                    topics.Add(new OffsetsResponse.Topic(topicName + t, partitionId, errorCode, _randomizer.Next(-1, int.MaxValue), version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null));
                }
            }
            var response = new OffsetsResponse(topics, version >= 2 ? (TimeSpan?)TimeSpan.FromMilliseconds(150) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetsResponseTopicEquality(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName,
            [Values(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
            )] ErrorCode errorCode)
        {
            var topic = new OffsetsResponse.Topic(topicName, 0, errorCode, _randomizer.Next(-1, int.MaxValue), version >= 1 ? (DateTimeOffset?) DateTimeOffset.UtcNow : null);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetsResponse.Topic(topicName + 1, 0, errorCode, topic.Offset, version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null),
                new OffsetsResponse.Topic(topicName, 1, errorCode, topic.Offset, version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null),
                new OffsetsResponse.Topic(topicName, 0, errorCode + 1, topic.Offset, version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null),
                new OffsetsResponse.Topic(topicName, 0, errorCode, topic.Offset + 1, version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null));
        }

        [Test]
        public void OffsetsResponseEquality(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName, 
            [Values(2)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorCode.NOT_LEADER_FOR_PARTITION
            )] ErrorCode errorCode, 
            [Values(2)] int offsetsPerPartition)
        {
            var topics = new List<OffsetsResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                for (var o = 0; o < offsetsPerPartition; o++) {
                    topics.Add(new OffsetsResponse.Topic(topicName + t, partitionId, errorCode, _randomizer.Next(-1, int.MaxValue), version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null));
                }
            }
            var alternate = topics.Take(1).Select(p => new OffsetsResponse.Topic(topicName, p.PartitionId, p.Error, p.Offset, p.Timestamp));
            var response = new OffsetsResponse(topics, version >= 2 ? (TimeSpan?)TimeSpan.FromMilliseconds(150) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new OffsetsResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime),
                new OffsetsResponse(topics.Skip(1), response.ThrottleTime)
                );
            if (version >= 2) {
                response.AssertNotEqual(new OffsetsResponse(topics, response.ThrottleTime.GetValueOrDefault().Add(TimeSpan.FromMilliseconds(100))));
            }
        }

        [Test]
        public void MetadataRequest(
            [Values(0, 1, 2, 3, 4)] short version,
            [Values("testTopic")] string topicName,
            [Values(0, 1, 10)] int topicsPerRequest)
        {
            var topics = new List<string>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(topicName + t);
            }
            var request = new MetadataRequest(topics, version >= 4 ? (bool?)true : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void MetadataRequestEquality(
            [Values(0, 1, 2, 3, 4)] short version,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest)
        {
            var topics = new List<string>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(topicName + t);
            }
            var alternate = topics.Take(1).Select(p => p + p);
            var request = new MetadataRequest(topics, version >= 4 ? (bool?)true : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(new MetadataRequest[] { null });
            request.AssertNotEqual(new MetadataRequest(alternate.Union(topics.Skip(1)), request.AllowTopicAutoCreation));
            request.AssertNotEqual(new MetadataRequest(topics.Skip(1), request.AllowTopicAutoCreation));
            if (version >= 4) {
                request.AssertNotEqual(new MetadataRequest(topics, false));
            }
        }

        [Test]
        public void MetadataResponse(
            [Values(0, 1, 2, 3, 4)] short version,
            [Values(15)] int brokersPerRequest,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
             )] ErrorCode errorCode)
        {
            var brokers = new List<Server>();
            for (var b = 0; b < brokersPerRequest; b++) {
                string rack = null;
                if (version >= 1) {
                    rack = "Rack" + b;
                }
                brokers.Add(new Server(b, "broker-" + b, 9092 + b, rack));
            }
            var topics = new List<MetadataResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitions = new List<MetadataResponse.Partition>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var leader = _randomizer.Next(0, brokersPerRequest - 1);
                    var replica = 0;
                    var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                    var isr = 0;
                    var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                    partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
                }
                topics.Add(new MetadataResponse.Topic(topicName + t, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null));
            }
            var response = new MetadataResponse(brokers, topics, version >= 1 ? brokersPerRequest : (int?)null, version >= 2 ? $"cluster-{version}" : null, version >= 3 ? (TimeSpan?)TimeSpan.FromMilliseconds(150) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void ServerEquality([Values(0, 1, 2, 3, 4)] short version)
        {
            var server = new Server(1, "kafka", 9092, version >= 1 ? "rack" : null);
            server.AssertEqualToSelf();
            server.AssertNotEqual(null,
                new Server(server.Id + 1, server.Host, server.Port, server.Rack),
                new Server(server.Id, server.Host + 1, server.Port, server.Rack),
                new Server(server.Id, server.Host, server.Port + 1, server.Rack)
                );
            if (version >= 1) {
                server.AssertNotEqual(
                    new Server(server.Id, server.Host, server.Port, server.Rack + 1),
                    new Server(server.Id, server.Host, server.Port, null));
            }
        }

        [Test]
        public void MetadataResponsePartitionEquality(
            [Values(15)] int brokersPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
             )] ErrorCode errorCode)
        {
            var leader = _randomizer.Next(0, brokersPerRequest - 1);
            var replica = 0;
            var replicas = _randomizer.Next(1, brokersPerRequest).Repeat(() => replica++).ToArray();
            var isr = 0;
            var isrs = _randomizer.Next(1, replica).Repeat(() => isr++).ToArray();
            var partition = new MetadataResponse.Partition(partitionsPerTopic, leader, errorCode, replicas, isrs);

            partition.AssertEqualToSelf();
            partition.AssertNotEqual(null,
                new MetadataResponse.Partition(partition.PartitionId + 1, leader, errorCode, replicas, isrs),
                new MetadataResponse.Partition(partition.PartitionId, leader + 1, errorCode, replicas, isrs),
                new MetadataResponse.Partition(partition.PartitionId, leader, errorCode + 1, replicas, isrs),
                new MetadataResponse.Partition(partition.PartitionId, leader, errorCode, replicas.Skip(1), isrs),
                new MetadataResponse.Partition(partition.PartitionId, leader, errorCode, (new [] { replicas.Count() * 2}).Union(replicas.Skip(1)), isrs),
                new MetadataResponse.Partition(partition.PartitionId, leader, errorCode, replicas, isrs.Skip(1)),
                new MetadataResponse.Partition(partition.PartitionId, leader, errorCode, replicas, (new [] { isrs.Count() * 2}).Union(isrs.Skip(1))));
        }

        [Test]
        public void MetadataResponseTopicEquality(
            [Values(0, 1, 2, 3, 4)] short version,
            [Values(15)] int brokersPerRequest,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
             )] ErrorCode errorCode)
        {
            var partitions = new List<MetadataResponse.Partition>();
            for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                var leader = _randomizer.Next(0, brokersPerRequest - 1);
                var replica = 0;
                var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                var isr = 0;
                var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
            }

            var alternate = partitions.Take(1).Select(p => new MetadataResponse.Partition(p.PartitionId + 1, p.Leader, p.PartitionError, p.Replicas, p.Isr));
            var topic = new MetadataResponse.Topic(topicName, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new MetadataResponse.Topic(topicName + 1, errorCode, partitions, topic.IsInternal),
                new MetadataResponse.Topic(topicName, errorCode + 1, partitions, topic.IsInternal),
                new MetadataResponse.Topic(topicName, errorCode, partitions.Skip(1), topic.IsInternal),
                new MetadataResponse.Topic(topicName, errorCode, alternate.Union(partitions.Skip(1)), topic.IsInternal)
                );
            if (version >= 1) {
                topic.AssertNotEqual(new MetadataResponse.Topic(topicName, errorCode, partitions, !topic.IsInternal.Value));
            }
        }

        [Test]
        public void MetadataResponseEquality(
            [Values(0, 1, 2, 3, 4)] short version,
            [Values(15)] int brokersPerRequest,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
             )] ErrorCode errorCode)
        {
            var brokers = new List<Server>();
            for (var b = 0; b < brokersPerRequest; b++) {
                string rack = null;
                if (version >= 1) {
                    rack = "Rack" + b;
                }
                brokers.Add(new Server(b, "broker-" + b, 9092 + b, rack));
            }
            var topics = new List<MetadataResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitions = new List<MetadataResponse.Partition>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var leader = _randomizer.Next(0, brokersPerRequest - 1);
                    var replica = 0;
                    var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                    var isr = 0;
                    var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                    partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
                }
                topics.Add(new MetadataResponse.Topic(topicName + t, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null));
            }
            var alternateBrokers = brokers.Take(1).Select(b => new Server(b.Id + 1, b.Host, b.Port, b.Rack));
            var alternateTopics = topics.Take(1).Select(t => new MetadataResponse.Topic(t.TopicName + t.TopicName, t.TopicError, t.PartitionMetadata, t.IsInternal));
            var response = new MetadataResponse(brokers, topics, version >= 1 ? brokersPerRequest : (int?)null, version >= 2 ? $"cluster-{version}" : null, version >= 4 ? (TimeSpan?)TimeSpan.FromMilliseconds(150) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new MetadataResponse(brokers.Skip(1), topics, response.ControllerId, response.ClusterId, response.ThrottleTime),
                new MetadataResponse(alternateBrokers.Union(brokers.Skip(1)), topics, response.ControllerId, response.ClusterId, response.ThrottleTime),
                new MetadataResponse(brokers, topics.Skip(1), response.ControllerId, response.ClusterId, response.ThrottleTime),
                new MetadataResponse(brokers, alternateTopics.Union(topics.Skip(1)), response.ControllerId, response.ClusterId, response.ThrottleTime)
                );
            if (version >= 1) {
                response.AssertNotEqual(new MetadataResponse(brokers, topics, response.ControllerId + 1, response.ClusterId, response.ThrottleTime));
            }
            if (version >= 2) {
                response.AssertNotEqual(new MetadataResponse(brokers, topics, response.ControllerId, response.ClusterId + 1, response.ThrottleTime));
            }
            if (version >= 3) {
                response.AssertNotEqual(new MetadataResponse(brokers, topics, response.ControllerId, response.ClusterId, TimeSpan.FromMilliseconds(250)));
            }
        }

        [Test]
        public void OffsetCommitRequest(
            [Values(0, 1, 2, 3)] short version,
            [Values("group1", "group2")] string groupId,
            [Values(0, 5)] int generation,
            [Values(-1, 20000)] int retentionTime,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int maxPartitions,
            [Values(10)] int maxOffsets,
            [Values(null, "something useful for the client")] string metadata)
        {
            var timestamp = retentionTime;
            var offsetCommits = new List<OffsetCommitRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                offsetCommits.Add(new OffsetCommitRequest.Topic(
                                      topicName + t,
                                      t%maxPartitions,
                                      _randomizer.Next(0, int.MaxValue),
                                      metadata,
                                      version == 1 ? timestamp : (long?)null));
            }
            var request = new OffsetCommitRequest(
                groupId,
                offsetCommits,
                version >= 1 ? "member" + generation : null,
                version >= 1 ? generation : 0,
                version >= 2 && retentionTime >= 0 ? (TimeSpan?) TimeSpan.FromMilliseconds(retentionTime) : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void OffsetCommitTopicEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values(-1, 20000)] int retentionTime,
            [Values("testTopic")] string topicName,
            [Values(5)] int maxPartitions,
            [Values(null, "something useful for the client")] string metadata)
        {
            var topic = new OffsetCommitRequest.Topic(topicName, maxPartitions, _randomizer.Next(0, int.MaxValue), metadata, version == 1 ? retentionTime : (long?) null);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetCommitRequest.Topic(topicName + 1, maxPartitions, topic.Offset, metadata, topic.TimeStamp),
                new OffsetCommitRequest.Topic(topicName, maxPartitions + 1, topic.Offset, metadata, topic.TimeStamp),
                new OffsetCommitRequest.Topic(topicName, maxPartitions, topic.Offset + 1, metadata, topic.TimeStamp),
                new OffsetCommitRequest.Topic(topicName, maxPartitions, topic.Offset, (metadata ?? "stuff") + 1, topic.TimeStamp)
                );
            if (version == 1) {
                topic.AssertNotEqual(new OffsetCommitRequest.Topic(topicName, maxPartitions, topic.Offset, metadata, retentionTime + 1));
            }
        }

        [Test]
        public void OffsetCommitRequestEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values("group")] string groupId,
            [Values(5)] int generation,
            [Values(20000)] int retentionTime,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int maxPartitions,
            [Values(10)] int maxOffsets,
            [Values("something useful for the client")] string metadata)
        {
            var timestamp = retentionTime;
            var offsetCommits = new List<OffsetCommitRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++)
            {
                offsetCommits.Add(new OffsetCommitRequest.Topic(
                    topicName + t,
                    t % maxPartitions,
                    _randomizer.Next(0, int.MaxValue),
                    metadata,
                    version == 1 ? timestamp : (long?)null));
            }
            var alternate = offsetCommits.Take(1).Select(o => new OffsetCommitRequest.Topic(topicName, o.PartitionId, o.Offset, o.Metadata, o.TimeStamp));
            var request = new OffsetCommitRequest(
                groupId,
                offsetCommits,
                version >= 1 ? "member" + generation : null,
                version >= 1 ? generation : 0,
                version >= 2 && retentionTime >= 0 ? (TimeSpan?)TimeSpan.FromMilliseconds(retentionTime) : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new OffsetCommitRequest(groupId + 1, offsetCommits, request.MemberId, request.GenerationId, request.RetentionTime),
                new OffsetCommitRequest(groupId, offsetCommits.Skip(1), request.MemberId, request.GenerationId, request.RetentionTime),
                new OffsetCommitRequest(groupId, alternate.Union(offsetCommits.Skip(1)), request.MemberId, request.GenerationId, request.RetentionTime),
                new OffsetCommitRequest(groupId, offsetCommits, request.MemberId + 1, request.GenerationId, request.RetentionTime),
                new OffsetCommitRequest(groupId, offsetCommits, request.MemberId, request.GenerationId + 1, request.RetentionTime)
            );
            if (version >= 1) {
                request.AssertNotEqual(
                    new OffsetCommitRequest(groupId, offsetCommits, request.MemberId + 1, request.GenerationId, request.RetentionTime),
                    new OffsetCommitRequest(groupId, offsetCommits, request.MemberId, request.GenerationId + 1, request.RetentionTime)
                );
            }
            if (version >= 2) {
                request.AssertNotEqual(
                    new OffsetCommitRequest(groupId, offsetCommits, request.MemberId, request.GenerationId, TimeSpan.FromMilliseconds(100))
                );
            }
        }

        [Test]
        public void OffsetCommitResponse(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var topics = new List<TopicResponse>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    topics.Add(new TopicResponse(topicName + t, partitionId, errorCode));
                }
            }
            var response = new OffsetCommitResponse(topics, version >= 3 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetCommitResponseEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var topics = new List<TopicResponse>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    topics.Add(new TopicResponse(topicName + t, partitionId, errorCode));
                }
            }
            var alternate = topics.Take(1).Select(o => new TopicResponse(topicName, o.PartitionId, o.Error));
            var response = new OffsetCommitResponse(topics, version >= 3 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new OffsetCommitResponse(topics.Skip(1), response.ThrottleTime),
                new OffsetCommitResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime)
            );
            if (version >= 3) {
                response.AssertNotEqual(new OffsetCommitResponse(topics, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void OffsetFetchRequest(
            [Values("group1", "group2")] string groupId,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int maxPartitions)
        {
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new TopicPartition(topicName + t, t % maxPartitions));
            }
            var request = new OffsetFetchRequest(groupId, topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void OffsetFetchRequestEquality(
            [Values("group")] string groupId,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int maxPartitions)
        {
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicsPerRequest; t++)
            {
                topics.Add(new TopicPartition(topicName + t, t % maxPartitions));
            }
            var alternate = topics.Take(1).Select(o => new TopicPartition(topicName, o.PartitionId));
            var request = new OffsetFetchRequest(groupId, topics);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new OffsetFetchRequest(groupId + 1, topics),
                new OffsetFetchRequest(groupId, topics.Skip(1)),
                new OffsetFetchRequest(groupId, alternate.Union(topics.Skip(1)))
            );
        }

        [Test]
        public void OffsetFetchResponse(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                 ErrorCode.GROUP_LOAD_IN_PROGRESS,
                 ErrorCode.NOT_COORDINATOR_FOR_GROUP,
                 ErrorCode.ILLEGAL_GENERATION,
                 ErrorCode.UNKNOWN_MEMBER_ID,
                 ErrorCode.TOPIC_AUTHORIZATION_FAILED,
                 ErrorCode.GROUP_AUTHORIZATION_FAILED
             )] ErrorCode errorCode)
        {
            var topics = new List<OffsetFetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
                    topics.Add(new OffsetFetchResponse.Topic(topicName + t, partitionId, errorCode, offset, offset >= 0 ? topicName : string.Empty));
                }
            }
            var response = new OffsetFetchResponse(topics, version >= 2 ? (ErrorCode?)errorCode : null, version >= 3 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetFetchResponseTopicEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                ErrorCode.GROUP_AUTHORIZATION_FAILED
            )] ErrorCode errorCode)
        {
            var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
            var topic = new OffsetFetchResponse.Topic(topicName, partitionsPerTopic, errorCode, offset, offset >= 0 ? topicName : string.Empty);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetFetchResponse.Topic(topic.TopicName + 1, topic.PartitionId, topic.Error, topic.Offset, topic.Metadata),
                new OffsetFetchResponse.Topic(topic.TopicName, topic.PartitionId + 1, topic.Error, topic.Offset, topic.Metadata),
                new OffsetFetchResponse.Topic(topic.TopicName, topic.PartitionId, topic.Error + 1, topic.Offset, topic.Metadata),
                new OffsetFetchResponse.Topic(topic.TopicName, topic.PartitionId, topic.Error, topic.Offset + 1, topic.Metadata),
                new OffsetFetchResponse.Topic(topic.TopicName, topic.PartitionId, topic.Error, topic.Offset, topic.Metadata + 1)
            );
        }

        [Test]
        public void OffsetFetchResponseEquality(
            [Values(0, 1, 2, 3)] short version,
            [Values("testTopic")] string topicName,
            [Values(10)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(
                ErrorCode.GROUP_AUTHORIZATION_FAILED
            )] ErrorCode errorCode)
        {
            var topics = new List<OffsetFetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++)
            {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++)
                {
                    var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
                    topics.Add(new OffsetFetchResponse.Topic(topicName + t, partitionId, errorCode, offset, offset >= 0 ? topicName : string.Empty));
                }
            }
            var alternate = topics.Take(1).Select(o => new OffsetFetchResponse.Topic(topicName, o.PartitionId, o.Error, o.Offset, o.Metadata));
            var response = new OffsetFetchResponse(topics, version >= 2 ? (ErrorCode?)errorCode : null, version >= 3 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new OffsetFetchResponse(topics.Skip(1), response.Error, response.ThrottleTime),
                new OffsetFetchResponse(alternate.Union(topics.Skip(1)), response.Error, response.ThrottleTime)
            );
            if (version >= 2) {
                response.AssertNotEqual(new OffsetFetchResponse(topics, response.Error.Value + 1, response.ThrottleTime));
            }
            if (version >= 3) {
                response.AssertNotEqual(new OffsetFetchResponse(topics, response.Error, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void FindCoordinatorRequest(
            [Values(0, 1)] short version,
            [Values("group1", "group2")] string groupId)
        {
            var request = new FindCoordinatorRequest(groupId, version >= 1 ? CoordinatorType.Transaction : CoordinatorType.Group);
            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void FindCoordinatorRequestEquality(
            [Values(0, 1)] short version,
            [Values("group")] string groupId)
        {
            var request = new FindCoordinatorRequest(groupId, version >= 1 ? CoordinatorType.Transaction : CoordinatorType.Group);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new FindCoordinatorRequest(groupId + 1, request.CoordinatorType)
            );
            if (version >= 1) {
                request.AssertNotEqual(null,
                    new FindCoordinatorRequest(groupId, CoordinatorType.Group)
                );
            }
        }

        [Test]
        public void FindCoordinatorResponse(
            [Values(0, 1)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE,
                 ErrorCode.GROUP_AUTHORIZATION_FAILED
             )] ErrorCode errorCode,
            [Values(0, 1)] int coordinatorId
            )
        {
            var response = new FindCoordinatorResponse(errorCode, coordinatorId, "broker-" + coordinatorId, 9092 + coordinatorId, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null, version >= 1 ? "foo" : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void FindCoordinatorResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.GROUP_AUTHORIZATION_FAILED
            )] ErrorCode errorCode,
            [Values(1)] int coordinatorId
        )
        {
            var response = new FindCoordinatorResponse(errorCode, coordinatorId, "broker-" + coordinatorId, 9092 + coordinatorId, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null, version >= 1 ? "foo" : null);

            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new FindCoordinatorResponse(errorCode + 1, response.Id, response.Host, response.Port, response.ThrottleTime, response.ErrorMessage),
                new FindCoordinatorResponse(errorCode, response.Id + 1, response.Host, response.Port, response.ThrottleTime, response.ErrorMessage),
                new FindCoordinatorResponse(errorCode, response.Id, response.Host + 1, response.Port, response.ThrottleTime, response.ErrorMessage),
                new FindCoordinatorResponse(errorCode, response.Id, response.Host, response.Port + 1, response.ThrottleTime, response.ErrorMessage)
            );
            if (version >= 1) {
                response.AssertNotEqual(
                    new FindCoordinatorResponse(errorCode, response.Id, response.Host, response.Port, TimeSpan.FromMilliseconds(200), response.ErrorMessage),
                    new FindCoordinatorResponse(errorCode, response.Id, response.Host, response.Port, response.ThrottleTime, response.ErrorMessage + 1));
            }
        }

        [Test]
        public void JoinGroupRequest(
            [Values(0, 1, 2)] short version,
            [Values("test", "group")] string groupId, 
            [Values(1, 20000)] int sessionTimeout,
            [Values("one", "two")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(10)] int protocolsPerRequest)
        {
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var bytes = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(bytes);
                protocols.Add(new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(sessionTimeout * 2) : null);

            var encoder = new ByteMembershipEncoder(protocolType);
            request.AssertCanEncodeDecodeRequest(version, encoder);
        }

        [Test]
        public void JoinGroupGroupProtocolEquality(
            [Values(10)] int protocolsPerRequest)
        {
            var bytes = new byte[protocolsPerRequest * 100];
            _randomizer.NextBytes(bytes);
            var protocol = new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata("known", new ArraySegment<byte>(bytes)));
            protocol.AssertEqualToSelf();
            protocol.AssertNotEqual(null,
                new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata(protocol.ProtocolName + 1, new ArraySegment<byte>(bytes))),
                new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata(protocol.ProtocolName, new ArraySegment<byte>(bytes, 1, bytes.Length - 1)))
                );
        }

        [Test]
        public void JoinGroupRequestEquality(
            [Values(0, 1, 2)] short version,
            [Values("test")] string groupId,
            [Values(20000)] int sessionTimeout,
            [Values("member")] string memberId,
            [Values("consumer", "other")] string protocolType,
            [Values(10)] int protocolsPerRequest)
        {
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++)
            {
                var bytes = new byte[protocolsPerRequest * 100];
                _randomizer.NextBytes(bytes);
                protocols.Add(new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var alternate = protocols.Take(1).Select(
                p => new JoinGroupRequest.GroupProtocol(
                    new ByteTypeMetadata(
                        p.ProtocolName + 1, new ArraySegment<byte>(new byte[] { 0 }))));
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(sessionTimeout * 2) : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new JoinGroupRequest(groupId + 1, request.SessionTimeout, request.MemberId, request.ProtocolType, request.GroupProtocols, request.RebalanceTimeout),
                new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout + 1), request.MemberId, request.ProtocolType, request.GroupProtocols, request.RebalanceTimeout),
                new JoinGroupRequest(groupId, request.SessionTimeout, request.MemberId + 1, request.ProtocolType, request.GroupProtocols, request.RebalanceTimeout),
                new JoinGroupRequest(groupId, request.SessionTimeout, request.MemberId, request.ProtocolType + 1, request.GroupProtocols, request.RebalanceTimeout),
                new JoinGroupRequest(groupId, request.SessionTimeout, request.MemberId, request.ProtocolType, request.GroupProtocols.Skip(1), request.RebalanceTimeout),
                new JoinGroupRequest(groupId, request.SessionTimeout, request.MemberId, request.ProtocolType, alternate.Union(request.GroupProtocols.Skip(1)), request.RebalanceTimeout)
            );
            if (version >= 1)
            {
                request.AssertNotEqual(
                    new JoinGroupRequest(groupId, request.SessionTimeout, request.MemberId, request.ProtocolType, request.GroupProtocols, TimeSpan.FromMilliseconds(sessionTimeout * 3))
                );
            }
        }

        [Test]
        public void JoinGroupResponse(
            [Values(0, 1, 2)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values(0, 20000)] int generationId,
            [Values("consumer", "other")] string protocol, 
            [Values("test")] string leaderId, 
            [Values("", "an existing member")] string memberId, 
            [Values(10)] int memberCount)
        {
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var bytes = new byte[memberCount*100];
                _randomizer.NextBytes(bytes);
                members.Add(new JoinGroupResponse.Member(memberId + m, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var response = new JoinGroupResponse(errorCode, generationId, "known", leaderId, memberId, members, version >= 2 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version, new ByteMembershipEncoder(protocol));
        }
        [Test]
        public void JoinGroupResponseMemberEquality(
            [Values("an existing member")] string memberId,
            [Values(10)] int memberCount)
        {
            var bytes = new byte[memberCount * 100];
            _randomizer.NextBytes(bytes);
            var member = new JoinGroupResponse.Member(memberId, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes)));

            member.AssertEqualToSelf();
            member.AssertNotEqual(null,
                new JoinGroupResponse.Member(memberId + 1, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))),
                new JoinGroupResponse.Member(memberId, new ByteTypeMetadata("known1", new ArraySegment<byte>(bytes))),
                new JoinGroupResponse.Member(memberId, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes, 1, bytes.Length - 1)))
            );
        }

        [Test]
        public void JoinGroupResponseEquality(
            [Values(0, 1, 2)] short version,
            [Values(
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode,
            [Values(20000)] int generationId,
            [Values("consumer")] string protocol,
            [Values("test")] string leaderId,
            [Values("an existing member")] string memberId,
            [Values(10)] int memberCount)
        {
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++)
            {
                var bytes = new byte[memberCount * 100];
                _randomizer.NextBytes(bytes);
                members.Add(new JoinGroupResponse.Member(memberId + m, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var alternate = members.Take(1).Select(
                m => new JoinGroupResponse.Member(m.MemberId + 1,
                    new ByteTypeMetadata("known", new ArraySegment<byte>(new byte[]{0}))));
            var response = new JoinGroupResponse(errorCode, generationId, "known", leaderId, memberId, members, version >= 2 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new JoinGroupResponse(errorCode + 1, generationId, response.GroupProtocol, response.LeaderId, response.MemberId, response.Members, response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId + 1, response.GroupProtocol, response.LeaderId, response.MemberId, response.Members, response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId, response.GroupProtocol + 1, response.LeaderId, response.MemberId, response.Members, response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId, response.GroupProtocol, response.LeaderId + 1, response.MemberId, response.Members, response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId, response.GroupProtocol, response.LeaderId, response.MemberId + 1, response.Members, response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId, response.GroupProtocol, response.LeaderId, response.MemberId, response.Members.Skip(1), response.ThrottleTime),
                new JoinGroupResponse(errorCode, generationId, response.GroupProtocol, response.LeaderId, response.MemberId, alternate.Union(response.Members.Skip(1)), response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(
                    new JoinGroupResponse(
                        errorCode, generationId, response.GroupProtocol + 1, response.LeaderId, response.MemberId,
                        response.Members, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void JoinConsumerGroupRequest(
            [Values("test")] string groupId, 
            [Values(1)] int sessionTimeout,
            [Values("an existing member")] string memberId, 
            [Values("mine", "yours")] string protocol, 
            [Values(10)] int protocolsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var userData = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ groupId, memberId, protocol }, protocol + p, new ArraySegment<byte>(userData), 0);
                protocols.Add(new JoinGroupRequest.GroupProtocol(metadata));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, ConsumerEncoder.Protocol, protocols);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Test]
        public void JoinConsumerGroupResponse(
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values(0, 1, 20000)] int generationId,
            [Values("consumer")] string protocol, 
            [Values("test")] string leaderId, 
            [Values("an existing member")] string memberId, 
            [Values(10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var userData = new byte[memberCount*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, leaderId }, protocol, new ArraySegment<byte>(userData), 0);
                members.Add(new JoinGroupResponse.Member(memberId + m, metadata));
            }
            var response = new JoinGroupResponse(errorCode, generationId, protocol, leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void HeartbeatRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
            request.AssertCanEncodeRequestDecodeResponse(0);
        }


        [Test]
        public void HeartbeatRequestEquality(
            [Values("test")] string groupId,
            [Values(20000)] int generationId,
            [Values("an existing member")] string memberId)
        {
            GroupRequest request = new HeartbeatRequest(groupId, generationId, memberId);
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                null,
                new HeartbeatRequest(groupId + 1, generationId, memberId),
                new HeartbeatRequest(groupId, generationId + 1, memberId),
                new HeartbeatRequest(groupId, generationId, memberId + 1)
            );
        }

        [Test]
        public void HeartbeatResponse(
            [Values(0, 1)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var response = new HeartbeatResponse(errorCode, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void HeartbeatResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var response = new HeartbeatResponse(errorCode, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new HeartbeatResponse(errorCode + 1, response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(new HeartbeatResponse(errorCode, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void LeaveGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values("", "an existing member")] string memberId)
        {
            var request = new LeaveGroupRequest(groupId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
            request.AssertCanEncodeRequestDecodeResponse(0);
        }

        [Test]
        public void LeaveGroupRequestEquality(
            [Values("test", "a groupId")] string groupId, 
            [Values("", "an existing member")] string memberId)
        {
            var request = new LeaveGroupRequest(groupId, memberId);
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                null,
                new LeaveGroupRequest(groupId + 1, memberId),
                new LeaveGroupRequest(groupId, memberId + 1)
            );
        }

        [Test]
        public void LeaveGroupResponse(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var response = new LeaveGroupResponse(errorCode, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void LeaveGroupResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var response = new LeaveGroupResponse(errorCode, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new LeaveGroupResponse(errorCode + 1, response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(new LeaveGroupResponse(errorCode, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void SyncGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var bytes = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(bytes);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, new ByteTypeAssignment(new ArraySegment<byte>(bytes))));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            var encoder = new ByteMembershipEncoder(protocolType);
            request.AssertCanEncodeDecodeRequest(0, encoder);
            request.AssertCanEncodeRequestDecodeResponse(0, encoder);
        }

        [Test]
        public void SyncGroupRequestEquality(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(5)] int assignmentsPerRequest)
        {
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var bytes = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(bytes);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, new ByteTypeAssignment(new ArraySegment<byte>(bytes))));
            }
            var alternate = assignments.Take(1).Select(
                a => new SyncGroupRequest.GroupAssignment(
                    protocolType, a.MemberAssignment));
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                null,
                new SyncGroupRequest(groupId + 1, generationId, memberId, assignments),
                new SyncGroupRequest(groupId, generationId + 1, memberId, assignments),
                new SyncGroupRequest(groupId, generationId, memberId + 1, assignments),
                new SyncGroupRequest(groupId, generationId, memberId, assignments.Skip(1)),
                new SyncGroupRequest(groupId, generationId, memberId, alternate.Union(assignments.Skip(1)))
            );
        }

        [Test]
        public void SyncGroupResponse(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var bytes = new byte[1000];
            _randomizer.NextBytes(bytes);
            var response = new SyncGroupResponse(errorCode, new ByteTypeAssignment(new ArraySegment<byte>(bytes)), version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version, new ByteMembershipEncoder("protocolType"));
        }

        [Test]
        public void SyncGroupResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode)
        {
            var bytes = new byte[1000];
            _randomizer.NextBytes(bytes);
            var response = new SyncGroupResponse(errorCode, new ByteTypeAssignment(new ArraySegment<byte>(bytes)), version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new SyncGroupResponse(errorCode + 1, response.MemberAssignment, response.ThrottleTime),
                new SyncGroupResponse(errorCode + 1, new ByteTypeAssignment(new ArraySegment<byte>(bytes, 1, bytes.Length - 1)), response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(new SyncGroupResponse(errorCode, response.MemberAssignment, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void SyncConsumerGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var topics = new List<TopicPartition>();
                for (var t = 0; t < assignmentsPerRequest; t++) {
                    topics.Add(new TopicPartition(groupId + t, t));
                }
                var userData = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(userData);
                var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, assignment));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, encoder);
            request.AssertCanEncodeRequestDecodeResponse(0, encoder);
        }

        [Test]
        public void SyncConsumerGroupResponse(
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var topics = new List<TopicPartition>();
            for (var t = 0; t < memberCount; t++) {
                topics.Add(new TopicPartition("topic foo" + t, t));
            }
            var userData = new byte[memberCount*100];
            _randomizer.NextBytes(userData);
            var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);
            var response = new SyncGroupResponse(errorCode, assignment);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void DescribeGroupsRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(1, 5, 10)] int count)
        {
            var groups = new string[count];
            for (var g = 0; g < count; g++) {
                groups[g] = groupId + g;
            }
            var request = new DescribeGroupsRequest(groups);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void DescribeGroupsRequestEquality(
            [Values("test", "a groupId")] string groupId, 
            [Values(5)] int count)
        {
            var groups = new string[count];
            for (var g = 0; g < count; g++) {
                groups[g] = groupId + g;
            }
            var alternate = groups.Take(1).Select(_ => groupId);
            var request = new DescribeGroupsRequest(groups);
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                null,
                new DescribeGroupsRequest(groups.Skip(1)),
                new DescribeGroupsRequest(alternate.Union(groups.Skip(1)))
            );
        }

        [Test]
        public void DescribeGroupsResponse(
            [Values(0, 1)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Values(2, 3)] int count,
            [Values(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.Dead)] string state, 
            [Values("consumer", "unknown")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var metadata = new byte[count*100];
                    var assignment = new byte[count*10];
                    _randomizer.NextBytes(metadata);
                    _randomizer.NextBytes(assignment);

                    members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version, new ByteMembershipEncoder(protocolType));
        }

        [Test]
        public void DescribeGroupsResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode,
            [Values("test")] string groupId, 
            [Values(3)] int count,
            [Values(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.Dead)] string state, 
            [Values("consumer")] string protocolType,
            [Values("good")] string protocol)
        {
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var metadata = new byte[count*100];
                    var assignment = new byte[count*10];
                    _randomizer.NextBytes(metadata);
                    _randomizer.NextBytes(assignment);

                    members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var alternate = groups.Take(1).Select(g => new DescribeGroupsResponse.Group(errorCode + 1, groupId, state, protocolType, protocol, g.Members));
            var response = new DescribeGroupsResponse(groups, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DescribeGroupsResponse(groups.Skip(1), response.ThrottleTime),
                new DescribeGroupsResponse(alternate.Union(groups.Skip(1)), response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(new DescribeGroupsResponse(groups, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void DescribeGroupsResponseMemberEquality(
            [Values(2, 3)] int count,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var metadata = new byte[count * 100];
            var assignment = new byte[count * 10];
            _randomizer.NextBytes(metadata);
            _randomizer.NextBytes(assignment);

            var member = new DescribeGroupsResponse.Member("member", "client", "host-", new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment)));
            member.AssertEqualToSelf();
            member.AssertNotEqual(null,
                new DescribeGroupsResponse.Member("member", "client", "host-", new ByteTypeMetadata(protocol + "other", new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))),
                new DescribeGroupsResponse.Member("member-other", "client", "host-", new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))),
                new DescribeGroupsResponse.Member("member", "client-other", "host-other", new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))),
                new DescribeGroupsResponse.Member("member", "client", "host-", new ByteTypeMetadata(protocol, new ArraySegment<byte>(assignment)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))),
                new DescribeGroupsResponse.Member("member", "client", "host-", new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(metadata))));
        }

        [Test]
        public void DescribeGroupsResponseGroupEquality(
            [Values(2, 3)] int count,
            [Values("good", "bad", "ugly")] string protocol,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode,
            [Values(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.Dead)] string state,
            [Values("consumer", "unknown")] string protocolType,
            [Values("test", "a groupId")] string groupId)
        {
            var members = new List<DescribeGroupsResponse.Member>();
            for (var m = 0; m < count; m++)
            {
                var metadata = new byte[count * 100];
                var assignment = new byte[count * 10];
                _randomizer.NextBytes(metadata);
                _randomizer.NextBytes(assignment);

                members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))));
            }
            var group = new DescribeGroupsResponse.Group(errorCode, groupId, state, protocolType, protocol, members);
            group.AssertEqualToSelf();
            group.AssertNotEqual(null,
                new DescribeGroupsResponse.Group(errorCode + 1, groupId, state, protocolType, protocol, members),
                new DescribeGroupsResponse.Group(errorCode, groupId + 1, state, protocolType, protocol, members),
                new DescribeGroupsResponse.Group(errorCode, groupId, state + 1, protocolType, protocol, members),
                new DescribeGroupsResponse.Group(errorCode, groupId, state, protocolType + 1, protocol, members),
                new DescribeGroupsResponse.Group(errorCode, groupId, state, protocolType, protocol + 1, members),
                new DescribeGroupsResponse.Group(errorCode, groupId, state, protocolType, protocol, members.Skip(1)));
        }

        [Test]
        public void DescribeConsumerGroupsResponse(
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Values(2, 3)] int count,
            [Values(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.AwaitingSync)] string state, 
            [Values("consumer")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var encoder = new ConsumerEncoder();
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var memberId = "member" + m;
                    var userData = new byte[count*100];
                    _randomizer.NextBytes(userData);
                    var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, memberId }, protocol, new ArraySegment<byte>(userData), 0);

                    var topics = new List<TopicPartition>();
                    for (var t = 0; t < count; t++) {
                        topics.Add(new TopicPartition("topic foo" + t, t));
                    }
                    var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);

                    members.Add(new DescribeGroupsResponse.Member(memberId, "client" + m, "host-" + m, metadata, assignment));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void ListGroupsRequest([Values(0, 1)] short version)
        {
            var request = new ListGroupsRequest();
            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void ListGroupsRequestEquality()
        {
            var request = new ListGroupsRequest();
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                new ListGroupsRequest[] { null }
            );
        }

        [Test]
        public void ListGroupsResponse(
            [Values(0, 1)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Values(2, 3)] int count,
            [Values("consumer")] string protocolType)
        {
            var groups = new ListGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                groups[g] = new ListGroupsResponse.Group(groupId + g, protocolType);
            }
            var response = new ListGroupsResponse(errorCode, groups, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void ListGroupsResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode,
            [Values("test")] string groupId, 
            [Values(3)] int count,
            [Values("consumer")] string protocolType)
        {
            var groups = new ListGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                groups[g] = new ListGroupsResponse.Group(groupId + g, protocolType);
            }
            var alternate = groups.Take(1).Select(g => new ListGroupsResponse.Group(groupId, g.ProtocolType));
            var response = new ListGroupsResponse(errorCode, groups, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new ListGroupsResponse(errorCode + 1, groups, response.ThrottleTime),
                new ListGroupsResponse(errorCode, groups.Skip(1), response.ThrottleTime),
                new ListGroupsResponse(errorCode, alternate.Union(groups.Skip(1)), response.ThrottleTime)
            );
            if (version >= 1) {
                response.AssertNotEqual(new ListGroupsResponse(errorCode, groups, TimeSpan.FromMilliseconds(200)));
            }
        }

        [Test]
        public void ListGroupsResponseGroupEquality(
            [Values("consumer", "unknown")] string protocolType,
            [Values("test", "a groupId")] string groupId)
        {
            var group = new ListGroupsResponse.Group(groupId, protocolType);
            group.AssertEqualToSelf();
            group.AssertNotEqual(null,
                new ListGroupsResponse.Group(groupId + 1, protocolType),
                new ListGroupsResponse.Group(groupId, protocolType + 1));
        }

        [Test]
        public void SaslHandshakeRequest(
            [Values("EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER")] string mechanism)
        {
            var request = new SaslHandshakeRequest(mechanism);

            request.AssertCanEncodeDecodeRequest(0);
            request.AssertCanEncodeRequestDecodeResponse(0);
        }

        [Test]
        public void SaslHandshakeRequestEquality(
            [Values("PLAIN")] string mechanism)
        {
            var request = new SaslHandshakeRequest(mechanism);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new SaslHandshakeRequest(mechanism + 1));
        }

        [Test]
        public void SaslHandshakeResponse(
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [Values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)] int count)
        {
            var mechanisms = new[] { "EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER" };
            var response = new SaslHandshakeResponse(errorCode, mechanisms.Take(count));

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SaslHandshakeResponseEquality(
            [Values(
                ErrorCode.NONE,
                ErrorCode.OFFSET_METADATA_TOO_LARGE
            )] ErrorCode errorCode,
            [Values(2)] int count)
        {
            var mechanisms = new[] { "EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER" };
            var response = new SaslHandshakeResponse(errorCode, mechanisms.Take(count));
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new SaslHandshakeResponse(errorCode + 1, mechanisms.Take(count)),
                new SaslHandshakeResponse(errorCode, mechanisms.Take(count + 1)),
                new SaslHandshakeResponse(errorCode, mechanisms.Skip(1).Take(count)));
        }

        [Test]
        public void ApiVersionsRequest()
        {
            var request = new ApiVersionsRequest();
            request.AssertCanEncodeDecodeRequest(0);
            request.AssertCanEncodeRequestDecodeResponse(0);
        }

        [Test]
        public void ApiVersionsRequestEquality()
        {
            var request = new ApiVersionsRequest();
            request.AssertEqualToSelf();
            request.AssertNotEqual(
                new ApiVersionsRequest[] {
                    null
                });
        }

        [Test]
        public void ApiVersionsResponse(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.BROKER_NOT_AVAILABLE
            )] ErrorCode errorCode
        )
        {
            var supported = new List<ApiVersionsResponse.VersionSupport>();
            for (short apiKey = 0; apiKey <= 18; apiKey++) {
                supported.Add(new ApiVersionsResponse.VersionSupport((ApiKey)apiKey, 0, (short)_randomizer.Next(0, 2)));
            }
            var response = new ApiVersionsResponse(errorCode, supported, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void ApiVersionsResponseVersionSupportEquality()
        {
            var supported = new ApiVersionsResponse.VersionSupport((ApiKey)1, 0, (short)_randomizer.Next(0, 2));
            supported.AssertEqualToSelf();
            supported.AssertNotEqual(null,
                new ApiVersionsResponse.VersionSupport(supported.ApiKey + 1, supported.MinVersion, supported.MaxVersion),
                new ApiVersionsResponse.VersionSupport(supported.ApiKey, (short) (supported.MinVersion + 1), supported.MaxVersion),
                new ApiVersionsResponse.VersionSupport(supported.ApiKey, supported.MinVersion, (short) (supported.MaxVersion + 1)));
        }

        [Test]
        public void ApiVersionsResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.BROKER_NOT_AVAILABLE
            )] ErrorCode errorCode
        )
        {
            var supported = new List<ApiVersionsResponse.VersionSupport>();
            for (short apiKey = 0; apiKey <= 18; apiKey++) {
                supported.Add(new ApiVersionsResponse.VersionSupport((ApiKey)apiKey, 0, (short)_randomizer.Next(0, 2)));
            }
            var alternate = supported.Take(1).Select(s => new ApiVersionsResponse.VersionSupport(s.ApiKey, (short) (s.MinVersion + 1), (short) (s.MaxVersion + 1)));
            var response = new ApiVersionsResponse(errorCode, supported, version >= 1 ? (TimeSpan?)TimeSpan.FromSeconds(1) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new ApiVersionsResponse(errorCode + 1, supported, response.ThrottleTime),
                new ApiVersionsResponse(errorCode, supported.Skip(1), response.ThrottleTime),
                new ApiVersionsResponse(errorCode, alternate.Union(supported.Skip(1)), response.ThrottleTime));
            if (version >= 1) {
                response.AssertNotEqual(
                    new ApiVersionsResponse(errorCode + 1, supported, TimeSpan.FromMilliseconds(10))
                );
            }
        }

        [Test]
        public void CreateTopicsRequest(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(1, 3)] short replicationFactor,
            [Values(0, 3)] int configCount,
            [Values(0, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                    configs = null;
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, partitionsPerTopic, replicationFactor, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), version >= 1 ? (bool?)(version == 1) : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void CreateTopicsRequestTopicEquality(
            [Values("testTopic")] string topicName,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(1, 3)] short replicationFactor,
            [Values(1, 3)] int configCount)
        {
            var configs = new Dictionary<string, string>();
            for (var c = 0; c < configCount; c++) {
                configs["config-" + c] = Guid.NewGuid().ToString("N");
            }
            if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                configs = null;
            }
            var topic = new CreateTopicsRequest.Topic(topicName, partitionsPerTopic, replicationFactor, configs);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new CreateTopicsRequest.Topic(topicName + 1, partitionsPerTopic, replicationFactor, configs),
                new CreateTopicsRequest.Topic(topicName, partitionsPerTopic + 1, replicationFactor, configs),
                new CreateTopicsRequest.Topic(topicName, partitionsPerTopic, (short)(replicationFactor + 1), configs),
                new CreateTopicsRequest.Topic(topicName, partitionsPerTopic, replicationFactor, configs.Skip(1)));

            var assignments = 2.Repeat(i => new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic, i.Repeat(x => x))).ToList();
            var alternate = assignments.Take(1).Select(i => new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic + 1, i.Replicas));
            var topicWithReplicas = new CreateTopicsRequest.Topic(topicName, assignments, configs);
            topicWithReplicas.AssertEqualToSelf();
            topicWithReplicas.AssertNotEqual(null,
                new CreateTopicsRequest.Topic(topicName + 1, assignments, configs),
                new CreateTopicsRequest.Topic(topicName, assignments.Skip(1), configs),
                new CreateTopicsRequest.Topic(topicName, alternate.Union(assignments.Skip(1)), configs));
        }

        [Test]
        public void CreateTopicsRequestReplicaAssignmentEquality([Values(5)] int partitionsPerTopic)
        {
            var replicas = partitionsPerTopic.Repeat(x => x).ToList();
            var alternate = replicas.Take(1).Select(_ => 2);
            var assignment = new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic, replicas);
            assignment.AssertEqualToSelf();
            assignment.AssertNotEqual(null,
                new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic + 1, replicas),
                new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic, replicas.Skip(1)),
                new CreateTopicsRequest.ReplicaAssignment(partitionsPerTopic, alternate.Union(replicas.Skip(1))));
        }

        [Test]
        public void CreateTopicsRequestEquality(
            [Values(0, 1, 2)] short version,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicsPerRequest,
            [Values(5)] int partitionsPerTopic,
            [Values(3)] short replicationFactor,
            [Values(3)] int configCount,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                    configs = null;
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, partitionsPerTopic, replicationFactor, configs));
            }
            var alternate = topics.Take(1).Select(t => new CreateTopicsRequest.Topic(topicName, t.NumPartitions, t.ReplicationFactor, t.Configs));
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), version >= 1 ? (bool?)true : null);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new CreateTopicsRequest(topics.Skip(1), TimeSpan.FromMilliseconds(timeoutMilliseconds), request.ValidateOnly),
                new CreateTopicsRequest(alternate.Union(topics.Skip(1)), TimeSpan.FromMilliseconds(timeoutMilliseconds), request.ValidateOnly),
                new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds + 1), request.ValidateOnly));
            if (version >= 1) {
                request.AssertNotEqual(
                    new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds), false));
            }
        }

        [Test]
        public void CreateTopicsExplicitRequest(
            [Values("testTopic")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(1, 3)] short replicationFactor,
            [Values(0, 3)] int configCount,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                    configs = null;
                }

                var assignments = new List<CreateTopicsRequest.ReplicaAssignment>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var replica = 0;
                    var replicas = _randomizer.Next(0, replicationFactor - 1).Repeat(() => replica++);
                    assignments.Add(new CreateTopicsRequest.ReplicaAssignment(partitionId, replicas));
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, assignments, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void CreateTopicsResponse(
            [Values(0, 1, 2)] short version,
            [Values(
                 ErrorCode.NONE,
                 ErrorCode.INVALID_TOPIC_EXCEPTION,
                ErrorCode.INVALID_PARTITIONS
             )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName, 
            [Values(1, 5, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode, version >= 1 ? topicName : null);
            }
            var response = new CreateTopicsResponse(topics, version >= 2 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void CreateTopicsResponseTopicEquality(
            [Values(
                ErrorCode.INVALID_PARTITIONS
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(null, "something went wrong")] string errorMessage)
        {
            var topic = new TopicsResponse.Topic(topicName, errorCode, errorMessage);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new TopicsResponse.Topic(topicName + 1, errorCode, errorMessage),
                new TopicsResponse.Topic(topicName, errorCode + 1, errorMessage),
                new TopicsResponse.Topic(topicName, errorCode, "something else"));
        }

        [Test]
        public void CreateTopicsResponseEquality(
            [Values(0, 1, 2)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.INVALID_TOPIC_EXCEPTION,
                ErrorCode.INVALID_PARTITIONS
            )] ErrorCode errorCode,
            [Values("test")] string topicName, 
            [Values(5)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var alternate = topics.Take(1).Select(t => new TopicsResponse.Topic(topicName, errorCode));
            var response = new CreateTopicsResponse(topics, version >= 2 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new CreateTopicsResponse(topics.Skip(1), response.ThrottleTime),
                new CreateTopicsResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime));
            if (version >= 2) {
                response.AssertNotEqual(new CreateTopicsResponse(topics, TimeSpan.FromSeconds(100)));
            }
        }

        [Test]
        public void DeleteTopicsRequest(
            [Values(0, 1)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int count,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new string[count];
            for (var t = 0; t < count; t++) {
                topics[t] = topicName + t;
            }
            var request = new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void DeleteTopicsRequestEquality(
            [Values(0, 1)] short version,
            [Values("test")] string topicName,
            [Values(3)] int count,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new string[count];
            for (var t = 0; t < count; t++) {
                topics[t] = topicName + t;
            }
            var alternate = topics.Take(1).Select(_ => topicName);
            var request = new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new DeleteTopicsRequest(topics.Skip(1), request.Timeout),
                new DeleteTopicsRequest(alternate.Union(topics.Skip(1)), request.Timeout),
                new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds + 1)));
        }

        [Test]
        public void DeleteTopicsResponse(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var response = new DeleteTopicsResponse(topics, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void DeleteTopicsResponseEquality(
            [Values(0, 1)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }

            var alternate = topics.Take(1).Select(t => new TopicsResponse.Topic(topicName, errorCode));
            var response = new DeleteTopicsResponse(topics, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(100) : null);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DeleteTopicsResponse(topics.Skip(1), response.ThrottleTime),
                new DeleteTopicsResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime));
            if (version >= 1) {
                response.AssertNotEqual(new DeleteTopicsResponse(topics, TimeSpan.FromSeconds(100)));
            }
        }

        [Test]
        public void DeleteRecordsRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int partitions,
            [Values(2, long.MaxValue)] long offset,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new DeleteRecordsRequest.Topic[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new DeleteRecordsRequest.Topic(topicName + t, t, offset);
            }
            var request = new DeleteRecordsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void DeleteRecordsRequestEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions,
            [Values(50000)] long offset,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new DeleteRecordsRequest.Topic[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new DeleteRecordsRequest.Topic(topicName + t, t, offset);
            }
            var alternate = topics.Take(1).Select(t => new DeleteRecordsRequest.Topic(topicName, t.PartitionId, t.Offset));
            var request = new DeleteRecordsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new DeleteRecordsRequest(topics.Skip(1), request.Timeout),
                new DeleteRecordsRequest(alternate.Union(topics.Skip(1)), request.Timeout),
                new DeleteRecordsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds + 1)));
        }

        [Test]
        public void DeleteRecordsRequestTopicEquality(
            [Values("test")] string topicName,
            [Values(50000)] long offset)
        {
            var topic = new DeleteRecordsRequest.Topic(topicName, 1, offset);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new DeleteRecordsRequest.Topic(topicName + 1, 1, offset),
                new DeleteRecordsRequest.Topic(topicName, 2, offset),
                new DeleteRecordsRequest.Topic(topicName, 1, offset + 1));
        }

        [Test]
        public void DeleteRecordsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new DeleteRecordsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new DeleteRecordsResponse.Topic(topicName + t, t, count, errorCode);
            }
            var response = new DeleteRecordsResponse(topics, TimeSpan.FromMilliseconds(100));

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void DeleteRecordsResponseTopicEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName)
        {
            var topic = new DeleteRecordsResponse.Topic(topicName, 1, 1234, errorCode);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new DeleteRecordsResponse.Topic(topicName + 1, topic.PartitionId, topic.LowWatermark, topic.Error),
                new DeleteRecordsResponse.Topic(topicName, topic.PartitionId + 1, topic.LowWatermark, topic.Error),
                new DeleteRecordsResponse.Topic(topicName, topic.PartitionId, topic.LowWatermark + 1, topic.Error),
                new DeleteRecordsResponse.Topic(topicName, topic.PartitionId, topic.LowWatermark, topic.Error + 1));
        }

        [Test]
        public void DeleteRecordsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new DeleteRecordsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new DeleteRecordsResponse.Topic(topicName + t, t, count, errorCode);
            }

            var alternate = topics.Take(1).Select(t => new DeleteRecordsResponse.Topic(topicName, t.PartitionId, t.LowWatermark, errorCode));
            var response = new DeleteRecordsResponse(topics, TimeSpan.FromMilliseconds(100));
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DeleteRecordsResponse(topics.Skip(1), response.ThrottleTime),
                new DeleteRecordsResponse(alternate.Union(topics.Skip(1)), response.ThrottleTime),
                new DeleteRecordsResponse(topics, TimeSpan.FromSeconds(100)));
        }

        [Test]
        public void InitProducerIdRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string id,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var request = new InitProducerIdRequest(id, TimeSpan.FromMilliseconds(timeoutMilliseconds));
            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void InitProducerIdRequestEquality(
            [Values("test")] string topicName,
            [Values("test")] string id,
            [Values(20000)] int timeoutMilliseconds)
        {
            var request = new InitProducerIdRequest(id, TimeSpan.FromMilliseconds(timeoutMilliseconds));
            request.AssertNotEqual(null,
                new InitProducerIdRequest(id + 1, request.TransactionTimeout),
                new InitProducerIdRequest(id, TimeSpan.FromMilliseconds(timeoutMilliseconds + 1)));
        }

        [Test]
        public void InitProducerIdResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 5, 11)] long producerId,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var response = new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), errorCode, producerId, (short)(producerId % 256));

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void InitProducerIdResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(5)] long producerId,
            [Values(20000)] int timeoutMilliseconds)
        {
            var response = new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), errorCode, producerId, (short)(producerId % 256));
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds + 1), errorCode, producerId, response.ProducerEpoch),
                new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), errorCode + 1, producerId, response.ProducerEpoch),
                new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), errorCode, producerId + 1, response.ProducerEpoch),
                new InitProducerIdResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), errorCode, producerId, (short)(response.ProducerEpoch + 1)));
        }

        [Test]
        public void OffsetForLeaderEpochRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int partitions,
            [Values(2, int.MaxValue)] int epoch,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new OffsetForLeaderEpochRequest.Topic[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new OffsetForLeaderEpochRequest.Topic(topicName + t, t, epoch);
            }
            var request = new OffsetForLeaderEpochRequest(topics);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void OffsetForLeaderEpochRequestEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions,
            [Values(50000)] int epoch,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new OffsetForLeaderEpochRequest.Topic[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new OffsetForLeaderEpochRequest.Topic(topicName + t, t, epoch);
            }
            var alternate = topics.Take(1).Select(t => new OffsetForLeaderEpochRequest.Topic(topicName, t.PartitionId, t.LeaderEpoch));
            var request = new OffsetForLeaderEpochRequest(topics);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new OffsetForLeaderEpochRequest(topics.Skip(1)),
                new OffsetForLeaderEpochRequest(alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void OffsetForLeaderEpochRequestTopicEquality(
            [Values("test")] string topicName,
            [Values(50000)] int epoch)
        {
            var topic = new OffsetForLeaderEpochRequest.Topic(topicName, 1, epoch);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetForLeaderEpochRequest.Topic(topicName + 1, 1, epoch),
                new OffsetForLeaderEpochRequest.Topic(topicName, 2, epoch),
                new OffsetForLeaderEpochRequest.Topic(topicName, 1, epoch + 1));
        }

        [Test]
        public void OffsetForLeaderEpochResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new OffsetForLeaderEpochResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new OffsetForLeaderEpochResponse.Topic(topicName + t, errorCode, t, count);
            }
            var response = new OffsetForLeaderEpochResponse(topics);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetForLeaderEpochResponseTopicEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName)
        {
            var topic = new OffsetForLeaderEpochResponse.Topic(topicName, errorCode, 1, 1234);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new OffsetForLeaderEpochResponse.Topic(topicName + 1, topic.Error, topic.PartitionId, topic.EndOffset),
                new OffsetForLeaderEpochResponse.Topic(topicName, topic.Error + 1, topic.PartitionId + 1, topic.EndOffset),
                new OffsetForLeaderEpochResponse.Topic(topicName, topic.Error, topic.PartitionId + 1, topic.EndOffset),
                new OffsetForLeaderEpochResponse.Topic(topicName, topic.Error, topic.PartitionId, topic.EndOffset + 1));
        }

        [Test]
        public void OffsetForLeaderEpochResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new OffsetForLeaderEpochResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new OffsetForLeaderEpochResponse.Topic(topicName + t, errorCode, t, count);
            }

            var alternate = topics.Take(1).Select(t => new OffsetForLeaderEpochResponse.Topic(topicName, errorCode, t.PartitionId, t.EndOffset));
            var response = new OffsetForLeaderEpochResponse(topics);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new OffsetForLeaderEpochResponse(topics.Skip(1)),
                new OffsetForLeaderEpochResponse(alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void AddPartitionsToTxnRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int partitions,
            [Values(2, short.MaxValue)] short epoch,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new TopicPartition[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicPartition(topicName + t, t);
            }
            var request = new AddPartitionsToTxnRequest(topicName, partitions, epoch, topics);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void AddPartitionsToTxnRequestEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions,
            [Values(5000)] short epoch,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new TopicPartition[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicPartition(topicName + t, t);
            }
            var alternate = topics.Take(1).Select(t => new TopicPartition(topicName, t.PartitionId));
            var request = new AddPartitionsToTxnRequest(topicName, partitions, epoch, topics);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new AddPartitionsToTxnRequest(topicName + 1, partitions, epoch, topics),
                new AddPartitionsToTxnRequest(topicName, partitions + 1, epoch, topics),
                new AddPartitionsToTxnRequest(topicName, partitions, (short)(epoch + 1), topics),
                new AddPartitionsToTxnRequest(topicName, partitions, epoch, topics.Skip(1)),
                new AddPartitionsToTxnRequest(topicName, partitions, epoch, alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void AddPartitionsToTxnResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new TopicResponse[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicResponse(topicName + t, t, errorCode);
            }
            var response = new AddPartitionsToTxnResponse(TimeSpan.FromMilliseconds(count), topics);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void AddPartitionsToTxnResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var topics = new TopicResponse[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicResponse(topicName + t, t, errorCode);
            }

            var alternate = topics.Take(1).Select(t => new TopicResponse(topicName, t.PartitionId, errorCode));
            var response = new AddPartitionsToTxnResponse(TimeSpan.FromMilliseconds(count), topics);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new AddPartitionsToTxnResponse(TimeSpan.FromMilliseconds(count + 1), topics),
                new AddPartitionsToTxnResponse(response.ThrottleTime.Value, topics.Skip(1)),
                new AddPartitionsToTxnResponse(response.ThrottleTime.Value, alternate.Union(topics.Skip(1))));
        }
        
        [Test]
        public void AddOffsetsToTxnRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int partitions,
            [Values(2, short.MaxValue)] short epoch,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var request = new AddOffsetsToTxnRequest(topicName, partitions, epoch, topicName);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void AddOffsetsToTxnRequestEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions,
            [Values(5000)] short epoch,
            [Values(20000)] int timeoutMilliseconds)
        {
            var request = new AddOffsetsToTxnRequest(topicName, partitions, epoch, topicName);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new AddOffsetsToTxnRequest(topicName + 1, partitions, epoch, topicName),
                new AddOffsetsToTxnRequest(topicName, partitions + 1, epoch, topicName),
                new AddOffsetsToTxnRequest(topicName, partitions, (short)(epoch + 1), topicName),
                new AddOffsetsToTxnRequest(topicName, partitions, epoch, topicName + 1));
        }

        [Test]
        public void AddOffsetsToTxnResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 5, 11)] int count)
        {
            var response = new AddOffsetsToTxnResponse(TimeSpan.FromMilliseconds(count), errorCode);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void AddOffsetsToTxnResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 5, 11)] int count)
        {
            var response = new AddOffsetsToTxnResponse(TimeSpan.FromMilliseconds(count), errorCode);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new AddOffsetsToTxnResponse(TimeSpan.FromMilliseconds(count + 1), errorCode),
                new AddOffsetsToTxnResponse(response.ThrottleTime.Value, errorCode + 1));
        }
        
        [Test]
        public void EndTxnRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(2, 3)] int partitions,
            [Values(2, short.MaxValue)] short epoch)
        {
            var request = new EndTxnRequest(topicName, partitions, epoch, partitions % 2 == 0);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void EndTxnRequestEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions,
            [Values(5000)] short epoch)
        {
            var request = new EndTxnRequest(topicName, partitions, epoch, partitions % 2 == 0);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new EndTxnRequest(topicName + 1, partitions, epoch, request.TransactionResult),
                new EndTxnRequest(topicName, partitions + 1, epoch, request.TransactionResult),
                new EndTxnRequest(topicName, partitions, (short)(epoch + 1), request.TransactionResult),
                new EndTxnRequest(topicName, partitions, epoch, !request.TransactionResult));
        }

        [Test]
        public void EndTxnResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 5, 11)] int count)
        {
            var response = new EndTxnResponse(TimeSpan.FromMilliseconds(count), errorCode);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void EndTxnResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 5, 11)] int count)
        {
            var response = new EndTxnResponse(TimeSpan.FromMilliseconds(count), errorCode);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new EndTxnResponse(TimeSpan.FromMilliseconds(count + 1), errorCode),
                new EndTxnResponse(response.ThrottleTime.Value, errorCode + 1));
        }

        [Test]
        public void WriteTxnMarkersRequest(
            [Values(0)] short version,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(4)] int markerCount,
            [Values(2, 3)] int partitions,
            [Values(2, int.MaxValue)] int epoch,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var markers = new WriteTxnMarkersRequest.TransactionMarker[markerCount];
            for (var m = 0; m < markerCount; m++) {
                var topics = new TopicPartition[partitions];
                for (var t = 0; t < partitions; t++) {
                    topics[t] = new TopicPartition(topicName + t, t);
                }
                markers[m] = new WriteTxnMarkersRequest.TransactionMarker(m, 0, m % 2 ==0, topics, m);
            }
            var request = new WriteTxnMarkersRequest(markers);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void WriteTxnMarkersRequestEquality(
            [Values("test")] string topicName,
            [Values(4)] int markerCount,
            [Values(3)] int partitions,
            [Values(50000)] int epoch,
            [Values(20000)] int timeoutMilliseconds)
        {
            var markers = new WriteTxnMarkersRequest.TransactionMarker[markerCount];
            for (var m = 0; m < markerCount; m++) {
                var topics = new TopicPartition[partitions];
                for (var t = 0; t < partitions; t++) {
                    topics[t] = new TopicPartition(topicName + t, t);
                }
                markers[m] = new WriteTxnMarkersRequest.TransactionMarker(m, 0, m % 2 == 0, topics, m);
            }
            var alternate = markers.Take(1).Select(t => new WriteTxnMarkersRequest.TransactionMarker(t.ProducerId + 1, t.ProducerEpoch, t.TransactionResult, t.Topics, t.CoordinatorEpoch));
            var request = new WriteTxnMarkersRequest(markers);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new WriteTxnMarkersRequest(markers.Skip(1)),
                new WriteTxnMarkersRequest(alternate.Union(markers.Skip(1))));
        }

        [Test]
        public void WriteTxnMarkersRequestTransactionMarkerEquality(
            [Values("test")] string topicName,
            [Values(4)] int markerCount,
            [Values(3)] int partitions,
            [Values(50000)] int epoch)
        {
            var topics = new TopicPartition[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicPartition(topicName + t, t);
            }
            var alternate = topics.Take(1).Select(t => new TopicPartition(topicName, t.PartitionId));
            var marker = new WriteTxnMarkersRequest.TransactionMarker(partitions, 0, true, topics, partitions);
            marker.AssertEqualToSelf();
            marker.AssertNotEqual(null,
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId + 1, marker.ProducerEpoch, marker.TransactionResult, topics, marker.CoordinatorEpoch),
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId, (short)(marker.ProducerEpoch + 1), marker.TransactionResult, topics, marker.CoordinatorEpoch),
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId, marker.ProducerEpoch, !marker.TransactionResult, topics, marker.CoordinatorEpoch),
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId, marker.ProducerEpoch, marker.TransactionResult, topics.Skip(1), marker.CoordinatorEpoch),
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId, marker.ProducerEpoch, marker.TransactionResult, alternate.Union(topics.Skip(1)), marker.CoordinatorEpoch),
                new WriteTxnMarkersRequest.TransactionMarker(marker.ProducerId, marker.ProducerEpoch, marker.TransactionResult, topics, marker.CoordinatorEpoch + 1));
        }

        [Test]
        public void WriteTxnMarkersResponse(
            [Values(0)] short version,
            [Values(4)] int markerCount,
            [Values(3)] int partitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var markers = new WriteTxnMarkersResponse.TransactionMarker[markerCount];
            for (var m = 0; m < markerCount; m++) {
                var topics = new TopicResponse[partitions];
                for (var t = 0; t < partitions; t++) {
                    topics[t] = new TopicResponse(topicName + t, t, errorCode);
                }
                markers[m] = new WriteTxnMarkersResponse.TransactionMarker(m, topics);
            }
            var response = new WriteTxnMarkersResponse(markers);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void WriteTxnMarkersResponseTransactionMarkerEquality(
            [Values(0)] short version,
            [Values(3)] int partitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName)
        {
            var topics = new TopicResponse[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicResponse(topicName + t, t, errorCode);
            }
            var alternate = topics.Take(1).Select(t => new TopicResponse(topicName, t.PartitionId, t.Error));
            var marker = new WriteTxnMarkersResponse.TransactionMarker(partitions, topics);
            marker.AssertEqualToSelf();
            marker.AssertNotEqual(null,
                new WriteTxnMarkersResponse.TransactionMarker(partitions + 1, topics),
                new WriteTxnMarkersResponse.TransactionMarker(partitions, topics.Skip(1)),
                new WriteTxnMarkersResponse.TransactionMarker(partitions, alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void WriteTxnMarkersResponseEquality(
            [Values(0)] short version,
            [Values(4)] int markerCount,
            [Values(3)] int partitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(1, 5, 11)] int count)
        {
            var markers = new WriteTxnMarkersResponse.TransactionMarker[markerCount];
            for (var m = 0; m < markerCount; m++) {
                var topics = new TopicResponse[partitions];
                for (var t = 0; t < partitions; t++) {
                    topics[t] = new TopicResponse(topicName + t, t, errorCode);
                }
                markers[m] = new WriteTxnMarkersResponse.TransactionMarker(m, topics);
            }
            var alternate = markers.Take(1).Select(t => new WriteTxnMarkersResponse.TransactionMarker(t.ProducerId + 1, t.Topics));
            var response = new WriteTxnMarkersResponse(markers);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new WriteTxnMarkersResponse(markers.Skip(1)),
                new WriteTxnMarkersResponse(alternate.Union(markers.Skip(1))));
        }
        
        [Test]
        public void TxnOffsetCommitRequest(
            [Values(0)] short version,
            [Values("group1", "group2")] string groupId,
            [Values(0, 5)] short epoch,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicCount,
            [Values(5)] int partitions,
            [Values(null, "something useful for the client")] string metadata)
        {
            var topics = new TxnOffsetCommitRequest.Topic[topicCount];
            for (var t = 0; t < topicCount; t++) {
                topics[t] = new TxnOffsetCommitRequest.Topic(topicName + t, t % partitions, _randomizer.Next(0, int.MaxValue), metadata);
            }
            var request = new TxnOffsetCommitRequest(topicName, partitions, epoch, groupId, topics);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void TxnOffsetCommitRequestEquality(
            [Values("group1")] string groupId,
            [Values(0, 5)] short epoch,
            [Values("testTopic")] string topicName,
            [Values(2)] int topicCount,
            [Values(5)] int partitions,
            [Values(null, "something useful for the client")] string metadata)
        {
            var topics = new TxnOffsetCommitRequest.Topic[topicCount];
            for (var t = 0; t < topicCount; t++) {
                topics[t] = new TxnOffsetCommitRequest.Topic(topicName + t, t % partitions, _randomizer.Next(0, int.MaxValue), metadata);
            }
            var alternate = topics.Take(1).Select(t => new TxnOffsetCommitRequest.Topic(topicName, t.PartitionId, t.Offset, t.Metadata));
            var request = new TxnOffsetCommitRequest(topicName, partitions, epoch, groupId, topics);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new TxnOffsetCommitRequest(topicName + 1, partitions, epoch, groupId, topics),
                new TxnOffsetCommitRequest(topicName, partitions + 1, epoch, groupId, topics),
                new TxnOffsetCommitRequest(topicName, partitions, (short)(epoch + 1), groupId, topics),
                new TxnOffsetCommitRequest(topicName, partitions, epoch, groupId + 1, topics),
                new TxnOffsetCommitRequest(topicName, partitions, epoch, groupId, topics.Skip(1)),
                new TxnOffsetCommitRequest(topicName, partitions, epoch, groupId, alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void TxnOffsetCommitRequestTopicEquality(
            [Values("test")] string topicName,
            [Values(3)] int partitions)
        {
            var topic = new TxnOffsetCommitRequest.Topic(topicName, partitions, _randomizer.Next(0, int.MaxValue), topicName);
            topic.AssertEqualToSelf();
            topic.AssertNotEqual(null,
                new TxnOffsetCommitRequest.Topic(topicName + 1, partitions, topic.Offset, topicName),
                new TxnOffsetCommitRequest.Topic(topicName, partitions + 1, topic.Offset, topicName),
                new TxnOffsetCommitRequest.Topic(topicName, partitions, topic.Offset + 1, topicName),
                new TxnOffsetCommitRequest.Topic(topicName, partitions, topic.Offset, (topicName ?? "stuff") + 1)
            );
        }

        [Test]
        public void TxnOffsetCommitResponse(
            [Values(0)] short version,
            [Values(3)] int partitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new TopicResponse[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicResponse(topicName + t, t, errorCode);
            }
            var response = new TxnOffsetCommitResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), topics);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void TxnOffsetCommitResponseEquality(
            [Values(0)] short version,
            [Values(3)] int partitions,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName,
            [Values(20000)] int timeoutMilliseconds)
        {
            var topics = new TopicResponse[partitions];
            for (var t = 0; t < partitions; t++) {
                topics[t] = new TopicResponse(topicName + t, t, errorCode);
            }
            var alternate = topics.Take(1).Select(t => new TopicResponse(topicName, t.PartitionId, t.Error));
            var response = new TxnOffsetCommitResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds), topics);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new TxnOffsetCommitResponse(TimeSpan.FromMilliseconds(timeoutMilliseconds + 1), topics),
                new TxnOffsetCommitResponse(response.ThrottleTime.Value, topics.Skip(1)),
                new TxnOffsetCommitResponse(response.ThrottleTime.Value, alternate.Union(topics.Skip(1))));
        }

        [Test]
        public void DescribeAclsRequest(
            [Values(0)] short version,
            [Values("test", "anotherName")] string name,
            [Values(2, 3)] byte operationOrType)
        {
            var request = new DescribeAclsRequest(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void DescribeAclsRequestEquality(
            [Values(0)] short version,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var request = new DescribeAclsRequest(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new DescribeAclsRequest((byte)(request.ResourceType + 1), request.ResourceName, request.Principal, request.Host, request.Operation, request.PermissionType),
                new DescribeAclsRequest(request.ResourceType, request.ResourceName + 1, request.Principal, request.Host, request.Operation, request.PermissionType),
                new DescribeAclsRequest(request.ResourceType, request.ResourceName, request.Principal + 1, request.Host, request.Operation, request.PermissionType),
                new DescribeAclsRequest(request.ResourceType, request.ResourceName, request.Principal, request.Host + 1, request.Operation, request.PermissionType),
                new DescribeAclsRequest(request.ResourceType, request.ResourceName, request.Principal, request.Host, (byte)(request.Operation + 1), request.PermissionType),
                new DescribeAclsRequest(request.ResourceType, request.ResourceName, request.Principal, request.Host, request.Operation, (byte)(request.PermissionType + 1)));
        }

        [Test]
        public void DescribeAclsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(3)] byte operationOrType,
            [Values(0, 1, 20000)] int timeout)
        {
            var resources = new AclResource[resourceCount];
            for (var r = 0; r < resourceCount; r++) {
                resources[r] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var response = new DescribeAclsResponse(TimeSpan.FromMilliseconds(timeout), errorCode, name, resources);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void DescribeAclsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(3)] byte operationOrType,
            [Values(20000)] int timeout)
        {
            var resources = new AclResource[resourceCount];
            for (var r = 0; r < resourceCount; r++) {
                resources[r] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var alternate = resources.Take(1).Select(r => new AclResource(r.ResourceType, r.ResourceName + 1, r.Principal, r.Host, r.Operation, r.PermissionType));
            var response = new DescribeAclsResponse(TimeSpan.FromMilliseconds(timeout), errorCode, name, resources);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DescribeAclsResponse(TimeSpan.FromMilliseconds(timeout + 1), response.Error, response.ErrorMessage, response.Resources),
                new DescribeAclsResponse(response.ThrottleTime.Value, response.Error + 1, response.ErrorMessage, response.Resources),
                new DescribeAclsResponse(response.ThrottleTime.Value, response.Error, response.ErrorMessage + 1, response.Resources),
                new DescribeAclsResponse(response.ThrottleTime.Value, response.Error, response.ErrorMessage, response.Resources.Skip(1)),
                new DescribeAclsResponse(response.ThrottleTime.Value, response.Error, response.ErrorMessage, alternate.Union(response.Resources.Skip(1))));
        }

        [Test]
        public void CreateAclsRequest(
            [Values(0)] short version,
            [Values(1, 5)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(2, 3)] byte operationOrType)
        {
            var resources = new AclResource[resourceCount];
            for (var r = 0; r < resourceCount; r++) {
                resources[r] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var request = new CreateAclsRequest(resources);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void AclResourceEquality(
            [Values(0)] short version,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resource = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new AclResource((byte)(resource.ResourceType + 1), resource.ResourceName, resource.Principal, resource.Host, resource.Operation, resource.PermissionType),
                new AclResource(resource.ResourceType, resource.ResourceName + 1, resource.Principal, resource.Host, resource.Operation, resource.PermissionType),
                new AclResource(resource.ResourceType, resource.ResourceName, resource.Principal + 1, resource.Host, resource.Operation, resource.PermissionType),
                new AclResource(resource.ResourceType, resource.ResourceName, resource.Principal, resource.Host + 1, resource.Operation, resource.PermissionType),
                new AclResource(resource.ResourceType, resource.ResourceName, resource.Principal, resource.Host, (byte)(resource.Operation + 1), resource.PermissionType),
                new AclResource(resource.ResourceType, resource.ResourceName, resource.Principal, resource.Host, resource.Operation, (byte)(resource.PermissionType + 1)));
        }

        [Test]
        public void CreateAclsRequestEquality(
            [Values(0)] short version,
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resources = new AclResource[resourceCount];
            for (var r = 0; r < resourceCount; r++) {
                resources[r] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var alternate = resources.Take(1).Select(r => new AclResource(r.ResourceType, r.ResourceName + 1, r.Principal, r.Host, r.Operation, r.PermissionType));
            var request = new CreateAclsRequest(resources);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new CreateAclsRequest(resources.Skip(1)),
                new CreateAclsRequest(alternate.Union(resources.Skip(1))));
        }

        [Test]
        public void CreateAclsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values("test", "anotherName")] string name,
            [Values(0, 1, 20000)] int timeout)
        {
            var errors = new CreateAclsResponse.ErrorResponse[count];
            for (var r = 0; r < count; r++) {
                errors[r] = new CreateAclsResponse.ErrorResponse(errorCode, name);
            }
            var response = new CreateAclsResponse(TimeSpan.FromMilliseconds(timeout), errors);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void CreateAclsResponseErrorResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values("test")] string name)
        {
            var resource = new CreateAclsResponse.ErrorResponse(errorCode, name);
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new CreateAclsResponse.ErrorResponse(errorCode + 1, name),
                new CreateAclsResponse.ErrorResponse(errorCode, name + 1));
        }

        [Test]
        public void CreateAclsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] int count,
            [Values("test", "anotherName")] string name,
            [Values(20000)] int timeout)
        {
            var errors = new CreateAclsResponse.ErrorResponse[count];
            for (var r = 0; r < count; r++) {
                errors[r] = new CreateAclsResponse.ErrorResponse(errorCode, name);
            }
            var alternate = errors.Take(1).Select(r => new CreateAclsResponse.ErrorResponse(r.Error + 1, r.ErrorMessage));
            var response = new CreateAclsResponse(TimeSpan.FromMilliseconds(timeout), errors);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new CreateAclsResponse(TimeSpan.FromMilliseconds(timeout + 1), errors),
                new CreateAclsResponse(response.ThrottleTime.Value, errors.Skip(1)),
                new CreateAclsResponse(response.ThrottleTime.Value, alternate.Union(errors.Skip(1))));
        }

        [Test]
        public void DeleteAclsRequest(
            [Values(0)] short version,
            [Values(1, 5)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(2, 3)] byte operationOrType)
        {
            var filters = new AclResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                filters[f] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var request = new DeleteAclsRequest(filters);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void DeleteAclsRequestEquality(
            [Values(0)] short version,
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var filters = new AclResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                filters[f] = new AclResource(operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var alternate = filters.Take(1).Select(r => new AclResource(r.ResourceType, r.ResourceName + 1, r.Principal, r.Host, r.Operation, r.PermissionType));
            var request = new DeleteAclsRequest(filters);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new DeleteAclsRequest(filters.Skip(1)),
                new DeleteAclsRequest(alternate.Union(filters.Skip(1))));
        }

        [Test]
        public void DeleteAclsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values("test", "anotherName")] string name,
            [Values(3)] byte operationOrType,
            [Values(0, 1, 20000)] int timeout)
        {
            var errors = new DeleteAclsResponse.FilterResponse[count];
            for (var r = 0; r < count; r++) {
                var acls = new DeleteAclsResponse.MatchingAcl[count];
                for (var a = 0; a < count; a++) {
                    acls[a] = new DeleteAclsResponse.MatchingAcl(errorCode, name, operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
                }
                errors[r] = new DeleteAclsResponse.FilterResponse(errorCode, name, acls);
            }
            var response = new DeleteAclsResponse(TimeSpan.FromMilliseconds(timeout), errors);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void DeleteAclsResponseMatchingAclEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] byte operationOrType,
            [Values("test")] string name)
        {
            var acl = new DeleteAclsResponse.MatchingAcl(errorCode, name, operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            acl.AssertEqualToSelf();
            acl.AssertNotEqual(null,
                new DeleteAclsResponse.MatchingAcl(errorCode + 1, acl.ErrorMessage, acl.ResourceType, acl.ResourceName, acl.Principal, acl.Host, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage + 1, acl.ResourceType, acl.ResourceName, acl.Principal, acl.Host, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, (byte)(acl.ResourceType + 1), acl.ResourceName, acl.Principal, acl.Host, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, acl.ResourceType, acl.ResourceName + 1, acl.Principal, acl.Host, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, acl.ResourceType, acl.ResourceName, acl.Principal + 1, acl.Host, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, acl.ResourceType, acl.ResourceName, acl.Principal, acl.Host + 1, acl.Operation, acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, acl.ResourceType, acl.ResourceName, acl.Principal, acl.Host, (byte)(acl.Operation + 1), acl.PermissionType),
                new DeleteAclsResponse.MatchingAcl(errorCode, acl.ErrorMessage, acl.ResourceType, acl.ResourceName, acl.Principal, acl.Host, acl.Operation, (byte)(acl.PermissionType + 1)));
        }
        
        [Test]
        public void DeleteAclsResponseFilterResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values(3)] byte operationOrType,
            [Values("test")] string name)
        {
            var acls = new DeleteAclsResponse.MatchingAcl[count];
            for (var a = 0; a < count; a++) {
                acls[a] = new DeleteAclsResponse.MatchingAcl(errorCode, name, operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
            }
            var alternate = acls.Take(1).Select(a => new DeleteAclsResponse.MatchingAcl(a.Error + 1, a.ErrorMessage, a.ResourceType, a.ResourceName, a.Principal, a.Host, a.Operation, a.PermissionType));
            var filter = new DeleteAclsResponse.FilterResponse(errorCode, name, acls);
            filter.AssertEqualToSelf();
            filter.AssertNotEqual(null,
                new DeleteAclsResponse.FilterResponse(errorCode + 1, name, acls),
                new DeleteAclsResponse.FilterResponse(errorCode, name + 1, acls),
                new DeleteAclsResponse.FilterResponse(errorCode, name, acls.Skip(1)),
                new DeleteAclsResponse.FilterResponse(errorCode, name, alternate.Union(acls.Skip(1))));
        }

        [Test]
        public void DeleteAclsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] int count,
            [Values(3)] byte operationOrType,
            [Values("test", "anotherName")] string name,
            [Values(20000)] int timeout)
        {
            var errors = new DeleteAclsResponse.FilterResponse[count];
            for (var r = 0; r < count; r++) {
                var acls = new DeleteAclsResponse.MatchingAcl[count];
                for (var a = 0; a < count; a++) {
                    acls[a] = new DeleteAclsResponse.MatchingAcl(errorCode, name, operationOrType, $"{name}resource", $"{name}principal", $"{name}host", operationOrType, operationOrType);
                }
                errors[r] = new DeleteAclsResponse.FilterResponse(errorCode, name, acls);
            }
            var alternate = errors.Take(1).Select(r => new DeleteAclsResponse.FilterResponse(r.Error + 1, r.ErrorMessage, r.MatchingAcls));
            var response = new DeleteAclsResponse(TimeSpan.FromMilliseconds(timeout), errors);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DeleteAclsResponse(TimeSpan.FromMilliseconds(timeout + 1), errors),
                new DeleteAclsResponse(response.ThrottleTime.Value, errors.Skip(1)),
                new DeleteAclsResponse(response.ThrottleTime.Value, alternate.Union(errors.Skip(1))));
        }

        [Test]
        public void DescribeConfigsRequest(
            [Values(0)] short version,
            [Values(1, 5)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(2, 3)] byte operationOrType)
        {
            var resources = new DescribeConfigsRequest.ConfigResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                resources[f] = new DescribeConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => name + i));
            }
            var request = new DescribeConfigsRequest(resources);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void DescribeConfigsRequestEquality(
            [Values(0)] short version,
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resources = new DescribeConfigsRequest.ConfigResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                resources[f] = new DescribeConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => name + i));
            }
            var alternate = resources.Take(1).Select(r => new DescribeConfigsRequest.ConfigResource(r.ResourceType, r.ResourceName + 1, r.ConfigNames));
            var request = new DescribeConfigsRequest(resources);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new DescribeConfigsRequest(resources.Skip(1)),
                new DescribeConfigsRequest(alternate.Union(resources.Skip(1))));
        }

        [Test]
        public void DescribeConfigsRequestConfigResourceEquality(
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resource = new DescribeConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => name + i));
            
            var alternate = resource.ConfigNames.Take(1).Select(r => name);
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new DescribeConfigsRequest.ConfigResource((byte)(resource.ResourceType + 1), resource.ResourceName, resource.ConfigNames),
                new DescribeConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName + 1, resource.ConfigNames),
                new DescribeConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName, resource.ConfigNames.Skip(1)),
                new DescribeConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName, alternate.Union(resource.ConfigNames.Skip(1))));
        }

        [Test]
        public void DescribeConfigsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values("test", "anotherName")] string name,
            [Values(3)] byte operationOrType,
            [Values(0, 1, 20000)] int timeout)
        {
            var resources = new DescribeConfigsResponse.ConfigResource[count];
            for (var r = 0; r < count; r++) {
                var entries = new DescribeConfigsResponse.ConfigEntry[count];
                for (var e = 0; e < count; e++) {
                    entries[e] = new DescribeConfigsResponse.ConfigEntry(name + e, name + e, e % 2 == 0, count % 2 == 0, e % 3 == 0);
                }
                resources[r] = new DescribeConfigsResponse.ConfigResource(errorCode, name, operationOrType, name, entries);
            }
            var response = new DescribeConfigsResponse(TimeSpan.FromMilliseconds(timeout), resources);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void DescribeConfigsResponseConfigEntryEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] byte operationOrType,
            [Values("test")] string name)
        {
            var entry = new DescribeConfigsResponse.ConfigEntry($"{name}name", $"{name}value", true, true, true);
            entry.AssertEqualToSelf();
            entry.AssertNotEqual(null,
                new DescribeConfigsResponse.ConfigEntry(entry.ConfigName + 1, entry.ConfigValue, entry.ReadOnly, entry.IsDefault, entry.IsSensitive),
                new DescribeConfigsResponse.ConfigEntry(entry.ConfigName, entry.ConfigValue + 1, entry.ReadOnly, entry.IsDefault, entry.IsSensitive),
                new DescribeConfigsResponse.ConfigEntry(entry.ConfigName, entry.ConfigValue, !entry.ReadOnly, entry.IsDefault, entry.IsSensitive),
                new DescribeConfigsResponse.ConfigEntry(entry.ConfigName, entry.ConfigValue, entry.ReadOnly, !entry.IsDefault, entry.IsSensitive),
                new DescribeConfigsResponse.ConfigEntry(entry.ConfigName, entry.ConfigValue, entry.ReadOnly, entry.IsDefault, !entry.IsSensitive));
        }
        
        [Test]
        public void DescribeConfigsResponseConfigResourceEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values(3)] byte operationOrType,
            [Values("test")] string name)
        {
            var entries = new DescribeConfigsResponse.ConfigEntry[count];
            for (var e = 0; e < count; e++) {
                entries[e] = new DescribeConfigsResponse.ConfigEntry(name + e, name + e, e % 2 == 0, count % 2 == 0, e % 3 == 0);
            }
            var alternate = entries.Take(1).Select(entry => new DescribeConfigsResponse.ConfigEntry(entry.ConfigName + 1, entry.ConfigValue, entry.ReadOnly, entry.IsDefault, entry.IsSensitive));
            var resource = new DescribeConfigsResponse.ConfigResource(errorCode, name, operationOrType, name, entries);
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new DescribeConfigsResponse.ConfigResource(errorCode + 1, name, operationOrType, name, entries),
                new DescribeConfigsResponse.ConfigResource(errorCode, name + 1, operationOrType, name, entries),
                new DescribeConfigsResponse.ConfigResource(errorCode, name, (byte)(operationOrType + 1), name, entries),
                new DescribeConfigsResponse.ConfigResource(errorCode, name, operationOrType, name + 1, entries),
                new DescribeConfigsResponse.ConfigResource(errorCode, name, operationOrType, name, entries.Skip(1)),
                new DescribeConfigsResponse.ConfigResource(errorCode, name, operationOrType, name, alternate.Union(entries.Skip(1))));
        }

        [Test]
        public void DescribeConfigsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] int count,
            [Values(3)] byte operationOrType,
            [Values("test", "anotherName")] string name,
            [Values(20000)] int timeout)
        {
            var resources = new DescribeConfigsResponse.ConfigResource[count];
            for (var r = 0; r < count; r++) {
                var entries = new DescribeConfigsResponse.ConfigEntry[count];
                for (var e = 0; e < count; e++) {
                    entries[e] = new DescribeConfigsResponse.ConfigEntry(name + e, name + e, e % 2 == 0, count % 2 == 0, e % 3 == 0);
                }
                resources[r] = new DescribeConfigsResponse.ConfigResource(errorCode, name + r, operationOrType, name, entries);
            }
            var alternate = resources.Take(1).Select(r => new DescribeConfigsResponse.ConfigResource(r.Error + 1, r.ErrorMessage, r.ResourceType, r.ResourceName, r.ConfigEntries));
            var response = new DescribeConfigsResponse(TimeSpan.FromMilliseconds(timeout), resources);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new DescribeConfigsResponse(TimeSpan.FromMilliseconds(timeout + 1), resources),
                new DescribeConfigsResponse(response.ThrottleTime.Value, resources.Skip(1)),
                new DescribeConfigsResponse(response.ThrottleTime.Value, alternate.Union(resources.Skip(1))));
        }
        
        [Test]
        public void AlterConfigsRequest(
            [Values(0)] short version,
            [Values(1, 5)] int resourceCount,
            [Values("test", "anotherName")] string name,
            [Values(2, 3)] byte operationOrType)
        {
            var resources = new AlterConfigsRequest.ConfigResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                resources[f] = new AlterConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => new ConfigEntry(name + i, name)));
            }
            var request = new AlterConfigsRequest(resources, operationOrType % 2 == 0);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void AlterConfigsRequestEquality(
            [Values(0)] short version,
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resources = new AlterConfigsRequest.ConfigResource[resourceCount];
            for (var f = 0; f < resourceCount; f++) {
                resources[f] = new AlterConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => new ConfigEntry(name + i, name)));
            }
            var alternate = resources.Take(1).Select(r => new AlterConfigsRequest.ConfigResource(r.ResourceType, r.ResourceName + 1, r.ConfigEntries));
            var request = new AlterConfigsRequest(resources, operationOrType % 2 == 0);
            request.AssertEqualToSelf();
            request.AssertNotEqual(null,
                new AlterConfigsRequest(resources.Skip(1), request.ValidateOnly),
                new AlterConfigsRequest(alternate.Union(resources.Skip(1)), request.ValidateOnly),
                new AlterConfigsRequest(resources, !request.ValidateOnly));
        }

        [Test]
        public void AlterConfigsRequestConfigResourceEquality(
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var resource = new AlterConfigsRequest.ConfigResource(operationOrType, $"{name}resource", resourceCount.Repeat(i => new ConfigEntry(name + i, name)));
            
            var alternate = resource.ConfigEntries.Take(1).Select(r => new ConfigEntry(name, r.ConfigValue));
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new AlterConfigsRequest.ConfigResource((byte)(resource.ResourceType + 1), resource.ResourceName, resource.ConfigEntries),
                new AlterConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName + 1, resource.ConfigEntries),
                new AlterConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName, resource.ConfigEntries.Skip(1)),
                new AlterConfigsRequest.ConfigResource(resource.ResourceType, resource.ResourceName, alternate.Union(resource.ConfigEntries.Skip(1))));
        }

        [Test]
        public void ConfigEntryEquality(
            [Values(3)] int resourceCount,
            [Values("test")] string name,
            [Values(3)] byte operationOrType)
        {
            var entry = new ConfigEntry(name + name, name);
            
            entry.AssertEqualToSelf();
            entry.AssertNotEqual(null,
                new ConfigEntry(entry.ConfigName + 1, entry.ConfigValue),
                new ConfigEntry(entry.ConfigName, entry.ConfigValue + 1));
        }

        [Test]
        public void AlterConfigsResponse(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values("test", "anotherName")] string name,
            [Values(3)] byte operationOrType,
            [Values(0, 1, 20000)] int timeout)
        {
            var resources = new ConfigResource[count];
            for (var r = 0; r < count; r++) {
                resources[r] = new ConfigResource(errorCode, name, operationOrType, name);
            }
            var response = new AlterConfigsResponse(TimeSpan.FromMilliseconds(timeout), resources);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void ConfigResourceEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(1, 10)] int count,
            [Values(3)] byte operationOrType,
            [Values("test")] string name)
        {
            var resource = new ConfigResource(errorCode, name, operationOrType, name);
            resource.AssertEqualToSelf();
            resource.AssertNotEqual(null,
                new ConfigResource(errorCode + 1, name, operationOrType, name),
                new ConfigResource(errorCode, name + 1, operationOrType, name),
                new ConfigResource(errorCode, name, (byte)(operationOrType + 1), name),
                new ConfigResource(errorCode, name, operationOrType, name + 1));
        }

        [Test]
        public void AlterConfigsResponseEquality(
            [Values(0)] short version,
            [Values(
                ErrorCode.NONE,
                ErrorCode.NOT_CONTROLLER
            )] ErrorCode errorCode,
            [Values(3)] int count,
            [Values(3)] byte operationOrType,
            [Values("test", "anotherName")] string name,
            [Values(20000)] int timeout)
        {
            var resources = new ConfigResource[count];
            for (var r = 0; r < count; r++) {
                resources[r] = new ConfigResource(errorCode, name, operationOrType, name);
            }
            var alternate = resources.Take(1).Select(r => new ConfigResource(r.Error + 1, r.ErrorMessage, r.ResourceType, r.ResourceName));
            var response = new AlterConfigsResponse(TimeSpan.FromMilliseconds(timeout), resources);
            response.AssertEqualToSelf();
            response.AssertNotEqual(null,
                new AlterConfigsResponse(TimeSpan.FromMilliseconds(timeout + 1), resources),
                new AlterConfigsResponse(response.ThrottleTime.Value, resources.Skip(1)),
                new AlterConfigsResponse(response.ThrottleTime.Value, alternate.Union(resources.Skip(1))));
        }

        #region Message Helpers

        private IEnumerable<Message> ModifyMessages(IEnumerable<Message> messages)
        {
            var first = true;
            foreach (var message in messages) {
                if (first) {
                    yield return new Message(message.Value, message.Key, message.Attribute, message.Offset + 1, message.Timestamp);
                    first = false;
                }
                yield return message;
            }
        }

        private IList<Message> GenerateMessages(int count, byte version, MessageCodec codec = MessageCodec.None)
        {
            var random = new Random(42);
            var messages = new List<Message>();
            for (var m = 0; m < count; m++) {
                var key = m > 0 ? new byte[8] : null;
                var value = new byte[8*(m + 1)];
                if (key != null) {
                    random.NextBytes(key);
                }
                random.NextBytes(value);

                var offset = codec == MessageCodec.None ? m*2 : m;
                messages.Add(new Message(new ArraySegment<byte>(value), key != null ? new ArraySegment<byte>(key) : new ArraySegment<byte>(), (byte)codec, offset, timestamp: version > 0 ? DateTimeOffset.UtcNow : (DateTimeOffset?)null));
            }
            return messages;
        }

        #endregion

#endregion
    }
}