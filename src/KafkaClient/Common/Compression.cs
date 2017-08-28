using System;
using System.IO;
using System.IO.Compression;
using KafkaClient.Protocol;

namespace KafkaClient.Common
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static CompressionLevel ZipLevel { get; set; } = CompressionLevel.Fastest;

        /// <summary>
        /// Write raw compressed bytes (no length prefix)
        /// </summary>
        public static void WriteCompressed(this IKafkaWriter writer, ArraySegment<byte> bytes, MessageCodec codec)
        {
            switch (codec) {
                case MessageCodec.Gzip:
                    using (var gzip = new GZipStream(writer.Stream, ZipLevel, true)) {
                        gzip.Write(bytes.Array, bytes.Offset, bytes.Count);
                        gzip.Flush();
                    }
                    break;

                case MessageCodec.Snappy:
#if DOTNETSTANDARD
                    var buffer = new byte[Snappy.SnappyCodec.GetMaxCompressedLength(bytes.Count)];
                    var size = Snappy.SnappyCodec.Compress(bytes.Array, bytes.Offset, bytes.Count, buffer, 0);
                    writer.Write(new ArraySegment<byte>(buffer, 0, size), false);
                    break;
#else
                    throw new NotSupportedException("Snappy codec is only supported on .net core");
#endif

                default:
                    throw new NotSupportedException($"Codec type of {codec} is not supported.");
            }
        }

        /// <summary>
        /// Read compressed bytes, and write uncompressed bytes.
        /// </summary>
        public static ArraySegment<byte> ToUncompressed(this ArraySegment<byte> source, MessageCodec codec)
        {
            switch (codec) {
                case MessageCodec.Gzip:
                    using (var writer = new KafkaWriter()) {
                        using (var gzip = new GZipStream(new MemoryStream(source.Array, source.Offset, source.Count), CompressionMode.Decompress)) {
                            gzip.CopyTo(writer.Stream);
                            gzip.Flush();
                        }
                        return writer.ToSegment(false);
                    }

                case MessageCodec.Snappy:
#if DOTNETSTANDARD
                    var length = Snappy.SnappyCodec.GetUncompressedLength(source.Array, source.Offset, source.Count);
                    var lengthBytes = length.ToBytes();
                    var buffer = new byte[lengthBytes.Length + length];
                    for (var i = 0; i < lengthBytes.Length; i++) {
                        buffer[i] = lengthBytes[i];
                    }
                    Snappy.SnappyCodec.Uncompress(source.Array, source.Offset, source.Count, buffer, lengthBytes.Length);
                    return new ArraySegment<byte>(buffer);
#else
                    throw new NotSupportedException("Snappy codec is only supported on .net core");
#endif

                default:
                    throw new NotSupportedException($"Codec type of {codec} is not supported.");
            }
        }
    }
}