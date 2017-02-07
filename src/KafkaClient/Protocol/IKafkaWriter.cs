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
        IKafkaWriter Write(long value);
        IKafkaWriter Write(string value);
        IKafkaWriter Write(ArraySegment<byte> value, bool includeLength = true);

        IDisposable MarkForLength();
        IDisposable MarkForCrc();

        ArraySegment<byte> ToSegment(bool includeLength = true);
        int Position { get; }

        Stream Stream { get; }
    }
}