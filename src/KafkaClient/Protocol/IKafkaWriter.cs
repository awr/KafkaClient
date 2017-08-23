using System;
using System.IO;

namespace KafkaClient.Protocol
{
    public interface IKafkaWriter : IDisposable
    {
        IKafkaWriter Write(bool value);
        IKafkaWriter Write(byte value);
        IKafkaWriter Write(short value);
        IKafkaWriter Write(int value);
        IKafkaWriter Write(uint value);
        IKafkaWriter Write(long value, bool varint = false);
        IKafkaWriter Write(string value, bool varint = false);
        IKafkaWriter Write(ArraySegment<byte> value, bool includeLength = true, bool varint = false);

        IDisposable MarkForLength(bool varint = false);
        IDisposable MarkForCrc(bool castagnoli = false);

        ArraySegment<byte> ToSegment(bool includeLength = true);
        int Position { get; }

        Stream Stream { get; }
    }
}