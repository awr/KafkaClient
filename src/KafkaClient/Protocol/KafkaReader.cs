﻿using System;
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
        public const int ResponseHeaderSize = KafkaWriter.IntegerByteSize + Request.CorrelationSize;
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

        public int ReadInt32()
        {
            return ToSegment(4).ToInt32();
        }

        public long ReadInt64()
        {
            return ToSegment(8).ToInt64();
        }

        public uint ReadUInt32()
        {
            return ToSegment(4).ToUInt32();
        }

        public string ReadString()
        {
            var size = ReadInt16();
            if (size == KafkaNullSize) return null;

            var segment = ReadSegment(size);
            var result = Encoding.UTF8.GetString(segment.Array, segment.Offset, segment.Count);
            return result;
        }

        public ArraySegment<byte> ReadBytes()
        {
            var size = ReadInt32();
            if (size == KafkaNullSize) { return EmptySegment; }

            var result = ReadSegment(size);
            return result;
        }

        public uint ReadCrc(int count)
        {
            if (count < 0) throw new EndOfStreamException();

            var segment = ToSegment(count, false);
            return Crc32.Compute(segment);
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

        public ArraySegment<byte> ReadSegment(int count)
        {
            return ToSegment(count);
        }

        public void Dispose()
        {
        }
    }
}