using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using KafkaClient.Testing;
using NUnit.Framework;

namespace KafkaClient.Tests
{
    public static class ProtocolAssertionExtensions
    {
        public static void AssertNotEqual<T>(this T first, params T[] others) where T : IEquatable<T>
        {
            foreach (var other in others) {
                Assert.AreNotEqual(first, other);
            }
        }

        public static void AssertEqualToSelf<T>(this T self) where T : IEquatable<T>
        {
            Assert.AreEqual(self, self);
            Assert.AreEqual(self.GetHashCode(), self.GetHashCode());
        }

        public static void AssertCanEncodeRequestDecodeResponse<T>(
            this IRequest<T> request, short version, IMembershipEncoder encoder = null)
            where T : class, IResponse
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null)
            {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(17, version, "Test-Request", encoders, encoder?.ProtocolType);
            var bytes = request.ToBytes(context);
            var decoded = request.ToResponse(context, bytes.Skip(4));
            Assert.NotNull(decoded);
        }

        public static void AssertCanEncodeDecodeRequest<T>(this T request, short version, IMembershipEncoder encoder = null, T forComparison = null) where T : class, IRequest
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(17, version, "Test-Request", encoders, encoder?.ProtocolType);
            var bytes = request.ToBytes(context);
            var decoded = KafkaDecoder.Decode<T>(bytes.Skip(4), context);

            if (forComparison == null) {
                forComparison = request;
            }
            Assert.AreEqual(forComparison.GetHashCode(), decoded.GetHashCode()); // HashCode equality
            Assert.AreEqual(forComparison.ShortString(), decoded.ShortString()); // ShortString equality
            var original = forComparison.ToString();
            var final = decoded.ToString();
            Assert.AreEqual(original, final); // ToString equality
            Assert.False(decoded.Equals(final)); // general equality test for sanity
            Assert.True(decoded.Equals(decoded)); // general equality test for sanity
            Assert.True(forComparison.Equals(decoded), $"Original\n{original}\nFinal\n{final}");
        }

        public static void AssertCanEncodeDecodeResponse<T>(this T response, short version, IMembershipEncoder encoder = null, T forComparison = null) where T : class, IResponse
        {
            var encoders = ImmutableDictionary<string, IMembershipEncoder>.Empty;
            if (encoder != null) {
                encoders = encoders.Add(encoder.ProtocolType, encoder);
            }

            var context = new RequestContext(16, version, "Test-Response", encoders, encoder?.ProtocolType);
            var data = KafkaDecoder.EncodeResponseBytes(context, response);
            var decoded = GetType<T>().ToResponse(context, data.Skip(Request.IntegerByteSize + Request.CorrelationSize));

            if (forComparison == null) {
                forComparison = response;
            }
            Assert.AreEqual(forComparison.GetHashCode(), decoded.GetHashCode()); // HashCode equality
            var original = forComparison.ToString();
            var final = decoded.ToString();
            Assert.AreEqual(original, final); // ToString equality
            Assert.False(decoded.Equals(final)); // general test for equality
            Assert.True(decoded.Equals(decoded)); // general equality test for sanity
            Assert.True(forComparison.Equals(decoded), $"Original\n{original}\nFinal\n{final}");
            Assert.True(forComparison.Errors.HasEqualElementsInOrder(decoded.Errors), "Errors");
        }

        public static ApiKey GetType<T>() where T : class, IResponse
        {
            if (typeof(T) == typeof(ProduceResponse)) return ApiKey.Produce;
            if (typeof(T) == typeof(FetchResponse)) return ApiKey.Fetch;
            if (typeof(T) == typeof(OffsetsResponse)) return ApiKey.Offsets;
            if (typeof(T) == typeof(MetadataResponse)) return ApiKey.Metadata;
            if (typeof(T) == typeof(OffsetCommitResponse)) return ApiKey.OffsetCommit;
            if (typeof(T) == typeof(OffsetFetchResponse)) return ApiKey.OffsetFetch;
            if (typeof(T) == typeof(GroupCoordinatorResponse)) return ApiKey.GroupCoordinator;
            if (typeof(T) == typeof(JoinGroupResponse)) return ApiKey.JoinGroup;
            if (typeof(T) == typeof(HeartbeatResponse)) return ApiKey.Heartbeat;
            if (typeof(T) == typeof(LeaveGroupResponse)) return ApiKey.LeaveGroup;
            if (typeof(T) == typeof(SyncGroupResponse)) return ApiKey.SyncGroup;
            if (typeof(T) == typeof(DescribeGroupsResponse)) return ApiKey.DescribeGroups;
            if (typeof(T) == typeof(ListGroupsResponse)) return ApiKey.ListGroups;
            if (typeof(T) == typeof(SaslHandshakeResponse)) return ApiKey.SaslHandshake;
            if (typeof(T) == typeof(ApiVersionsResponse)) return ApiKey.ApiVersions;
            if (typeof(T) == typeof(CreateTopicsResponse)) return ApiKey.CreateTopics;
            if (typeof(T) == typeof(DeleteTopicsResponse)) return ApiKey.DeleteTopics;
            throw new InvalidOperationException();
        }

    }
}