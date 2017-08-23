using System;

namespace KafkaClient.Protocol
{
    public interface IKafkaReader : IDisposable
    {
        bool ReadBoolean();
        byte ReadByte();
        short ReadInt16();
        int ReadInt32(bool varint = false);
        uint ReadUInt32();
        long ReadInt64(bool varint = false);
        string ReadString(bool varint = false);
        ArraySegment<byte> ReadBytes(bool varint = false);
        uint ReadCrc(int count, bool castagnoli = false);

        ArraySegment<byte> ReadSegment(int count);
        int Position { get; set; }

        bool HasBytes(int count);
    }
}