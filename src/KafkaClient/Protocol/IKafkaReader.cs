using System;

namespace KafkaClient.Protocol
{
    public interface IKafkaReader : IDisposable
    {
        bool ReadBoolean();
        byte ReadByte();
        short ReadInt16();
        int ReadInt32();
        int ReadVarint32();
        uint ReadUInt32();
        long ReadInt64();
        long ReadVarint64();
        string ReadString(int? length = null);
        ArraySegment<byte> ReadBytes(int count);
        ArraySegment<byte> ReadBytes();
        uint ReadCrc(int count, bool castagnoli = false);

        int Position { get; set; }

        bool HasBytes(int count);
    }
}