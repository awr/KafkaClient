﻿using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_tests.Fakes
{
    public class FakeKafkaConnection : IKafkaConnection
    {
        public Func<Task<ProduceResponse>> ProduceResponseFunction;
        public Func<Task<MetadataResponse>> MetadataResponseFunction;
        public Func<Task<OffsetResponse>> OffsetResponseFunction;
        public Func<Task<FetchResponse>> FetchResponseFunction;

        public FakeKafkaConnection(Uri address)
        {
            Endpoint = new DefaultKafkaConnectionFactory().Resolve(address, new DefaultTraceLog());
        }

        public long MetadataRequestCallCount; // { get; set; }
        public long ProduceRequestCallCount; //{ get; set; }
        public long OffsetRequestCallCount; //{ get; set; }
        public long FetchRequestCallCount; // { get; set; }

        public KafkaEndpoint Endpoint { get; private set; }

        public bool ReadPolling => true;

        public Task SendAsync(KafkaDataPayload payload, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async Task<T> SendAsync<T>(IKafkaRequest<T> request, IRequestContext context = null) where T : class, IKafkaResponse
        {
            T result;

            if (typeof(T) == typeof(ProduceResponse))
            {
                Interlocked.Increment(ref ProduceRequestCallCount);
                result = (T)((object)await ProduceResponseFunction());
            }
            else if (typeof(T) == typeof(MetadataResponse))
            {
                Interlocked.Increment(ref MetadataRequestCallCount);
                result = (T)(object)await MetadataResponseFunction();
            }
            else if (typeof(T) == typeof(OffsetResponse))
            {
                Interlocked.Increment(ref OffsetRequestCallCount);
                result = (T)(object)await OffsetResponseFunction();
            }
            else if (typeof(T) == typeof(FetchResponse))
            {
                Interlocked.Increment(ref FetchRequestCallCount);
                result = (T)(object)await FetchResponseFunction();
            }
            else
            {
                throw new NotImplementedException(typeof(T).FullName);
            }
            return result;
        }

        public void Dispose()
        {
        }
    }
}