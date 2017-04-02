using System.IO;
using System.Linq;
using KafkaClient.Protocol;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    public class KafkaWriterTests
    {
        // validates my assumptions about the default implementation doing the opposite of this implementation
        [Theory]
        [InlineData(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [InlineData(1, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [InlineData(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [InlineData(int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [InlineData(int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(int number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.Equal(expectedBytes, actualBytes);
        }

        [Theory]
        [InlineData((short)0, new byte[] { 0x00, 0x00 })]
        [InlineData((short)1, new byte[] { 0x00, 0x01 })]
        [InlineData((short)256, new byte[] { 0x01, 0x00 })]
        [InlineData((short)16295, new byte[] { 0x3F, 0xA7 })]
        [InlineData((short)(-1), new byte[] { 0xFF, 0xFF })]
        [InlineData(short.MinValue, new byte[] { 0x80, 0x00 })]
        [InlineData(short.MaxValue, new byte[] { 0x7F, 0xFF })]
        public void Int16Tests(short number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.Equal(expectedBytes, actualBytes.ToArray());
        }

        [Theory]
        [InlineData(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [InlineData(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [InlineData(256, new byte[] { 0x00, 0x00, 0x01, 0x00 })]
        [InlineData(258, new byte[] { 0x00, 0x00, 0x01, 0x02 })]
        [InlineData(67305985, new byte[] { 0x04, 0x03, 0x02, 0x01 })]
        [InlineData(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [InlineData(int.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [InlineData(int.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(int number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.Equal(expectedBytes, actualBytes.ToArray());
        }

        [Theory]
        [InlineData(0L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [InlineData(1L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [InlineData(258L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02 })]
        [InlineData(1234567890123L, new byte[] { 0x00, 0x00, 0x01, 0x1F, 0x71, 0xFB, 0x04, 0xCB })]
        [InlineData(-1L, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [InlineData(long.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [InlineData(long.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void Int64Tests(long number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.Equal(expectedBytes, actualBytes.ToArray());
        }

        [Theory]
        [InlineData((uint)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [InlineData((uint)1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [InlineData((uint)123456789, new byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [InlineData((uint)0xffffffff, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(uint number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.Equal(expectedBytes, actualBytes.ToArray());
        }

        [Theory]
        [InlineData("0000", new byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [InlineData("€€€€", new byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        [InlineData("", new byte[] { 0x00, 0x00 })]
        [InlineData(null, new byte[] { 0xFF, 0xFF })]
        public void StringTests(string value, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(value);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.Equal(expectedBytes, actualBytes.ToArray());
        }
    }
}