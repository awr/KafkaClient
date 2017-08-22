using System;
using System.IO.Compression;
using System.Linq;
using BenchmarkDotNet.Attributes;
using KafkaClient.Protocol;

namespace KafkaClient.Performance
{
    public class ProduceBenchmark
    {
        [Params(100, 10000)]
        public int Messages { get; set; }

        [Params(1, 1000)]
        public int MessageSize { get; set; }

        [Params(MessageCodec.Gzip, MessageCodec.Snappy)]
        public MessageCodec Codec { get; set; }

        [Params(CompressionLevel.Fastest)]
        public CompressionLevel Level { get; set; }

        public int Partitions { get; } = 1;

        public short Version { get; } = 0;

        private ProduceRequest _request;

        [GlobalSetup]
        public void SetupData()
        {
            Common.Compression.ZipLevel = Level;
            _request = new ProduceRequest(
                        Enumerable.Range(1, Partitions)
                                  .Select(partitionId => new ProduceRequest.Topic(
                                      "topic", 
                                      partitionId, 
                                      Enumerable.Range(1, Messages)
                                                .Select(i => new Message(GenerateMessageBytes(), new ArraySegment<byte>(), (byte) Codec)), 
                                      Codec)));
        }

        private ArraySegment<byte> GenerateMessageBytes()
        {
            var buffer = new byte[MessageSize];
            new Random(42).NextBytes(buffer);
            return new ArraySegment<byte>(buffer);
        }

        [Benchmark]
        public ArraySegment<byte> Encode()
        {
            return _request.ToBytes(new RequestContext(1, Version));
        }
    }
}