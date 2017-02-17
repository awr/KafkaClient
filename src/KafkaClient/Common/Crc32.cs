// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

using System;

namespace KafkaClient.Common
{
    /// <summary>
    /// This code was originally from the copyrighted code listed above but was modified significantly
    /// as the original code was not thread safe and did not match was was required of this driver. This
    /// class now provides a static lib which will do the simple CRC calculation required by Kafka servers.
    /// </summary>
    public static class Crc32
    {
        private const uint DefaultPolynomial = 0xedb88320u;
        private const uint DefaultSeed = 0xffffffffu;
        private static readonly uint[] PolynomialTable;

        static Crc32()
        {
            PolynomialTable = InitializeTable();
        }

        public static uint Compute(ArraySegment<byte> bytes)
        {
            var crc = DefaultSeed;
            var max = bytes.Offset + bytes.Count;
            for (var i = bytes.Offset; i < max; i++) {
                crc = (crc >> 8) ^ PolynomialTable[bytes.Array[i] ^ crc & 0xff];
            }
            return ~crc;
        }

        private static uint[] InitializeTable()
        {
            var table = new uint[256];
            for (var i = 0; i < 256; i++) {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++) {
                    if ((entry & 1) == 1) {
                        entry = (entry >> 1) ^ DefaultPolynomial;
                    } else {
                        entry = entry >> 1;
                    }
                }
                table[i] = entry;
            }

            return table;
        }
    }
}