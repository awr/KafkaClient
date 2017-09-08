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
        IKafkaWriter Write(long value);
        IKafkaWriter Write(string value, bool varint = false);
        IKafkaWriter Write(ArraySegment<byte> value, bool includeLength = true);
        IKafkaWriter WriteVarint(uint value);
        IKafkaWriter WriteVarint(long value);

        IDisposable MarkForVarintLength(int expectedLength);
        IDisposable MarkForLength();
        IDisposable MarkForCrc(bool castagnoli = false);

        ArraySegment<byte> ToSegment(bool includeLength = true);
        int Position { get; }

        Stream Stream { get; }
    }
}