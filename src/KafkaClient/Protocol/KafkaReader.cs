using System;
using System.IO;
using System.Text;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// A BinaryReader that is BigEndian aware binary reader.
    /// </summary>
    /// <remarks>
    /// Booleans, bytes and byte arrays will be written directly.
    /// All other values will be converted to a byte array in BigEndian byte order and written.
    /// Characters and Strings will all be encoded in UTF-8 (which is byte order independent).
    /// </remarks>
    /// <remarks>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    ///
    /// The code was modified to provide Kafka specific logic and helper functions.
    /// Specifically where STRINGS are 16bit prefixed and BYTE[] are 32bit prefixed
    /// </remarks>
    public class KafkaReader : IKafkaReader
    {
        private const int KafkaNullSize = -1;
        private readonly ArraySegment<byte> _bytes;

        public KafkaReader(ArraySegment<byte> bytes)
        {
            _bytes = bytes;
        }

        public int Length => _bytes.Count;
        public int Position { get; set; }

        public bool HasBytes(int count)
        {
            return Position + count <= Length;
        }

        public bool ReadBoolean()
        {
            return ReadByte() > 0;
        }

        public byte ReadByte()
        {
            var result = _bytes.Array[_bytes.Offset + Position];
            Position += 1;
            return result;
        }

        public short ReadInt16()
        {
            return ToSegment(2).ToInt16();
        }

        public int ReadVarint32()
        {
            return (int) ReadVarint64();
        }

        public int ReadInt32()
        {
            return ToSegment(4).ToInt32();
        }

        public long ReadVarint64()
        {
            var segment = new ArraySegment<byte>(_bytes.Array, _bytes.Offset + Position, Length - Position);
            (int count, long value) = segment.FromVarint();
            Position += count;
            return value;
        }

        public long ReadInt64()
        {
            return ToSegment(8).ToInt64();
        }

        public uint ReadUInt32()
        {
            return ToSegment(4).ToUInt32();
        }

        public static int MaxArraySize = 1000;
        public static int MaxByteSize = 1000000;

        public void AssertMaxArraySize(int size)
        {
            if (size > MaxArraySize) throw new BufferUnderRunException($"Cannot allocate an array of size {size} (> {MaxArraySize}). Configure this through {nameof(KafkaReader)}.{nameof(MaxArraySize)}.");
        }

        private void AssertMaxBytes(int byteSize)
        {
            if (byteSize > MaxByteSize) throw new BufferUnderRunException($"Cannot allocate an array of size {byteSize} bytes (> {MaxByteSize}). Configure this through {nameof(KafkaReader)}.{nameof(MaxByteSize)}.");
        }

        public string ReadString(int? length = null)
        {
            if (!length.HasValue) {
                var size = ReadInt16();
                if (size == KafkaNullSize) return null;
                length = size;
            }

            var segment = ReadBytes(length.Value);
            var result = Encoding.UTF8.GetString(segment.Array, segment.Offset, segment.Count);
            return result;
        }

        public ArraySegment<byte> ReadBytes()
        {
            return ReadBytes(ReadInt32());
        }

        public ArraySegment<byte> ReadBytes(int count)
        {
            if (count == KafkaNullSize) { return EmptySegment; }
            AssertMaxBytes(count);
            return ToSegment(count);
        }

        public uint ReadCrc(int count, bool castagnoli = false)
        {
            if (count < 0) throw new EndOfStreamException();

            var segment = ToSegment(count, false);
            return Crc32.Compute(segment, castagnoli);
        }

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        private ArraySegment<byte> ToSegment(int count, bool movePosition = true)
        {
            var next = Position + count;
            if (count < 0 || Length < next) throw new EndOfStreamException();

            var current = Position;
            if (movePosition) {
                Position = next;
            }
            return new ArraySegment<byte>(_bytes.Array, _bytes.Offset + current, count);
        }

        public void Dispose()
        {
        }
    }
}