using System;
using System.IO;
using System.Text;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public class KafkaWriter : IKafkaWriter
    {
        private readonly MemoryStream _stream;

        public KafkaWriter()
        {
            _stream = new MemoryStream();
            Write((int) Request.IntegerByteSize); //pre-allocate space for buffer length
        }

        public IKafkaWriter Write(bool value)
        {
            _stream.WriteByte(value ? (byte)1 : (byte)0);
            return this;
        }

        public IKafkaWriter Write(byte value)
        {
            _stream.WriteByte(value);
            return this;
        }

        public IKafkaWriter Write(short value)
        {
            _stream.Write(value.ToBytes(), 0, 2);
            return this;
        }

        public IKafkaWriter Write(int value)
        {
            _stream.Write(value.ToBytes(), 0, 4);
            return this;
        }

        public IKafkaWriter Write(uint value)
        {
            _stream.Write(value.ToBytes(), 0, 4);
            return this;
        }

        public IKafkaWriter Write(long value)
        {
            _stream.Write(value.ToBytes(), 0, 8);
            return this;
        }

        public IKafkaWriter Write(ArraySegment<byte> value, bool includeLength = true)
        {
            if (value.Count == 0) {
                if (includeLength) {
                    Write(-1);
                }
                return this;
            }

            if (includeLength) {
                Write(value.Count);
            }
            _stream.Write(value.Array, value.Offset, value.Count);
            return this;
        }

        public IKafkaWriter Write(string value)
        {
            if (value == null) {
                Write((short)-1);
                return this;
            }

            var bytes = Encoding.UTF8.GetBytes(value); 
            Write((short)bytes.Length);
            _stream.Write(bytes, 0, bytes.Length);
            return this;
        }

        public ArraySegment<byte> ToSegment(bool includeLength = true)
        {
            if (includeLength) {
                WriteLength(0);
                return ToSegment(0);
            }
            return ToSegment((int) Request.IntegerByteSize);
        }

        private ArraySegment<byte> ToSegment(int offset)
        {
            var length = _stream.Length - offset;
            if (length < 0) throw new EndOfStreamException($"Cannot get offset {offset} past end of stream");

            ArraySegment<byte> segment;
            if (!_stream.TryGetBuffer(out segment)) {
                // the stream is a memorystream, always owning its own buffer
                throw new NotSupportedException();
            }

            return segment.Skip(offset);
        }

        private void WriteLength(int offset)
        {
            var length = (int)_stream.Length - (offset + Request.IntegerByteSize); 
            _stream.Position = offset;
            Write(length);
        }

        private void WriteCrc(int offset)
        {
            uint crc;
            ArraySegment<byte> segment;
            var computeFrom = offset + Request.IntegerByteSize;
            if (!_stream.TryGetBuffer(out segment)) {
                // the stream is a memorystream, always owning its own buffer
                throw new NotSupportedException();
            }

            crc = Crc32.Compute(segment.Skip(computeFrom));
            _stream.Position = offset;
            Write(crc);            
        }

        public IDisposable MarkForLength()
        {
            var markerPosition = (int)_stream.Position;
            _stream.Seek(Request.IntegerByteSize, SeekOrigin.Current); //pre-allocate space for marker
            return new WriteAt(this, WriteLength, markerPosition);
        }

        public IDisposable MarkForCrc()
        {
            var markerPosition = (int)_stream.Position;
            _stream.Seek(Request.IntegerByteSize, SeekOrigin.Current); //pre-allocate space for marker
            return new WriteAt(this, WriteCrc, markerPosition);
        }

        private class WriteAt : IDisposable
        {
            private readonly KafkaWriter _writer;
            private readonly int _position;
            private readonly Action<int> _write;

            public WriteAt(KafkaWriter writer, Action<int> write, int position)
            {
                _writer = writer;
                _position = position;
                _write = write;
            }

            public void Dispose()
            {
                _write(_position);
                _writer._stream.Seek(0, SeekOrigin.End);
            }
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public int Position => (int)_stream.Position;

        public Stream Stream => _stream;
    }
}