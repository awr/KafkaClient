﻿using KafkaNet.Protocol;

namespace kafka_tests
{
    public static class RequestFactory
    {
        public static ProduceRequest CreateProduceRequest(string topic, string message, string key = null)
        {
            return new ProduceRequest(new Payload(topic, 0, new[] {new Message(message, key)}));
        }

        public static FetchRequest CreateFetchRequest(string topic, int offset, int partitionId = 0)
        {
            return new FetchRequest(new Fetch(topic, partitionId, offset));
        }

        public static OffsetRequest CreateOffsetRequest(string topic, int partitionId = 0, int maxOffsets = 1, int time = -1)
        {
            return new OffsetRequest(new Offset(topic, partitionId, time, maxOffsets));
        }

        public static OffsetFetchRequest CreateOffsetFetchRequest(string topic, int partitionId = 0)
        {
            return new OffsetFetchRequest("DefaultGroup", new Topic(topic, partitionId));
        }
    }
}