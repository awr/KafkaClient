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
        [Fact]
        [TestCase(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(1, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [TestCase(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [TestCase(int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(int number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Fact]
        [TestCase((short)0, new byte[] { 0x00, 0x00 })]
        [TestCase((short)1, new byte[] { 0x00, 0x01 })]
        [TestCase((short)256, new byte[] { 0x01, 0x00 })]
        [TestCase((short)16295, new byte[] { 0x3F, 0xA7 })]
        [TestCase((short)(-1), new byte[] { 0xFF, 0xFF })]
        [TestCase(short.MinValue, new byte[] { 0x80, 0x00 })]
        [TestCase(short.MaxValue, new byte[] { 0x7F, 0xFF })]
        public void Int16Tests(short number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.That(actualBytes.ToArray(), Is.EqualTo(expectedBytes));
        }

        [Fact]
        [TestCase(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase(256, new byte[] { 0x00, 0x00, 0x01, 0x00 })]
        [TestCase(258, new byte[] { 0x00, 0x00, 0x01, 0x02 })]
        [TestCase(67305985, new byte[] { 0x04, 0x03, 0x02, 0x01 })]
        [TestCase(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(int.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(int number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.That(actualBytes.ToArray(), Is.EqualTo(expectedBytes));
        }

        [Fact]
        [TestCase(0L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(1L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 })]
        [TestCase(258L, new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02 })]
        [TestCase(1234567890123L, new byte[] { 0x00, 0x00, 0x01, 0x1F, 0x71, 0xFB, 0x04, 0xCB })]
        [TestCase(-1L, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(long.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(long.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        public void Int64Tests(long number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.That(actualBytes.ToArray(), Is.EqualTo(expectedBytes));
        }

        [Fact]
        [TestCase((uint)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((uint)1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((uint)123456789, new byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [TestCase((uint)0xffffffff, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(uint number, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(number);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.That(actualBytes.ToArray(), Is.EqualTo(expectedBytes));
        }

        [Fact]
        [TestCase("0000", new byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("€€€€", new byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        [TestCase("", new byte[] { 0x00, 0x00 })]
        [TestCase(null, new byte[] { 0xFF, 0xFF })]
        public void StringTests(string value, byte[] expectedBytes)
        {
            // arrange
            var writer = new KafkaWriter();

            // act
            writer.Write(value);

            // assert
            var actualBytes = writer.ToSegment(false);
            Assert.That(actualBytes.ToArray(), Is.EqualTo(expectedBytes));
        }
    }
}