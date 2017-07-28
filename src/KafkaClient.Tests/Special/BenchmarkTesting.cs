using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Testing;
using NUnit.Framework;

namespace KafkaClient.Tests.Special
{
    [Category("Benchmark")]
    internal class BenchmarkTesting
    {
        [Test]
        public void FetchSize()
        {
            int partitions = 1;
            short version = 0;
            byte messageVersion = 0;
            
            var results = new List<object>();
            foreach (var codec in new[] { MessageCodec.None, MessageCodec.Gzip, MessageCodec.Snappy }) {
                foreach (var messages in new[] { 100, 10000 }) {
                    foreach (var messageSize in new[] { 1, 1000 }) {
                        foreach (var level in new[] { CompressionLevel.Fastest }) {
                            Compression.ZipLevel = level;
                            var response = new FetchResponse(
                                Enumerable.Range(1, partitions)
                                          .Select(partitionId => new FetchResponse.Topic(
                                              "topic", 
                                              partitionId, 
                                              500,
                                              ErrorCode.NONE,
                                              messages: Enumerable.Range(1, messages)
                                                        .Select(i => new Message(GenerateMessageBytes(messageSize), new ArraySegment<byte>(), (byte) codec, version: messageVersion))
                                          )));
                            var bytes = KafkaDecoder.EncodeResponseBytes(new RequestContext(1, version), response);
                            var decoded = FetchResponse.FromBytes(new RequestContext(1, version), bytes.Skip(Request.IntegerByteSize + Request.CorrelationSize));
                            Assert.AreEqual(decoded.Responses.Sum(t => t.Messages.Count), response.Responses.Sum(t => t.Messages.Count));
                            var result = new {
                                Codec = codec.ToString(),
                                Level = codec == MessageCodec.None ? "-" : level.ToString(),
                                Messages = messages,
                                MessageSize = messageSize,
                                Bytes = bytes.Count
                            };
                            results.Add(result);
                        }
                    }
                }
            }

            WriteResults(results);
        }

        [Test]
        public void ProduceSize()
        {
            int partitions = 1;
            short version = 0;
            byte messageVersion = 0;

            var results = new List<object>();
            foreach (var codec in new[] { MessageCodec.None, MessageCodec.Gzip, MessageCodec.Snappy }) {
                foreach (var messages in new[] { 100, 10000 }) {
                    foreach (var messageSize in new[] { 1, 1000 }) {
                        foreach (var level in new[] { CompressionLevel.Fastest }) {
                            Compression.ZipLevel = level;
                            var request = new ProduceRequest(
                                        Enumerable.Range(1, partitions)
                                                  .Select(partitionId => new ProduceRequest.Topic(
                                                      "topic", 
                                                      partitionId, 
                                                      Enumerable.Range(1, messages)
                                                                .Select(i => new Message(GenerateMessageBytes(messageSize), new ArraySegment<byte>(), 0, version: messageVersion)), 
                                                      codec)));

                            var result = new {
                                Codec = codec.ToString(),
                                Level = codec == MessageCodec.None ? "-" : level.ToString(),
                                Messages = messages,
                                MessageSize = messageSize,
                                Bytes = request.ToBytes(new RequestContext(1, version)).Count
                            };
                            results.Add(result);
                        }
                    }
                }
            }

            WriteResults(results);
        }
        
        private ArraySegment<byte> GenerateMessageBytes(int messageSize)
        {
            var buffer = new byte[messageSize];
            new Random(42).NextBytes(buffer);
            return new ArraySegment<byte>(buffer);
        }

        private void WriteResults(List<object> results)
        {
            var output = new List<Tuple<string, List<string>, int>>();
            if (results == null || results.Count == 0) return;
            var type = results[0].GetType();
            foreach (var p in type.GetRuntimeProperties()) {
                var values = results.Select(result => p.GetValue(result).ToString()).ToList();
                output.Add(new Tuple<string, List<string>, int>(p.Name, values, Math.Max(p.Name.Length, values.Select(v => v.Length).Max())));
            }

            Console.WriteLine(FormatRow(output.Select(r => new Tuple<string, int>(r.Item1, r.Item3))));
            Console.WriteLine(FormatRow(output.Select(r => new Tuple<string, int>("", r.Item3)), '-'));
            for (var i = 0; i < results.Count; i++) {
                Console.WriteLine(FormatRow(output.Select(r => new Tuple<string, int>(r.Item2[i], r.Item3))));
            }
        }

        private string FormatRow(IEnumerable<Tuple<string, int>> values, char padding = ' ')
        {
            var buffer = new StringBuilder();
            foreach (var value in values) {
                if (buffer.Length == 0) {
                    buffer.Append(padding)
                          .Append(value.Item1.PadRight(value.Item2, padding));
                } else {
                    buffer.Append(padding)
                          .Append(value.Item1.PadLeft(value.Item2, padding));
                }
                buffer.Append(" |");
            }
            return buffer.ToString();
        }
    }
}