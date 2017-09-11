using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ApiVersions Response => error_code [api_versions] *throttle_time_ms 
    /// </summary>
    /// <remarks>
    /// ApiVersions Response => error_code [api_versions] *throttle_time_ms 
    ///   error_code => INT16
    ///   api_versions => api_key min_version max_version 
    ///     api_key => INT16
    ///     min_version => INT16
    ///     max_version => INT16
    ///   throttle_time_ms => INT32
    ///
    /// Version 1+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    /// </remarks>
    public class ApiVersionsResponse : ThrottledResponse, IResponse, IEquatable<ApiVersionsResponse>
    {
        public override string ToString() => $"{{error_code:{Error},api_versions:[{ApiVersions.ToStrings()}],{this.ThrottleToString()}}}";

        public static ApiVersionsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                var apiKeyCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(apiKeyCount);
                var apiKeys = new VersionSupport[apiKeyCount];
                for (var i = 0; i < apiKeyCount; i++) {
                    var apiKey = (ApiKey)reader.ReadInt16();
                    var minVersion = reader.ReadInt16();
                    var maxVersion = reader.ReadInt16();
                    apiKeys[i] = new VersionSupport(apiKey, minVersion, maxVersion);
                }
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                return new ApiVersionsResponse(errorCode, apiKeys, throttleTime);
            }
        }

        public ApiVersionsResponse(ErrorCode errorCode = ErrorCode.NONE, IEnumerable<VersionSupport> supportedVersions = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            ApiVersions = supportedVersions.ToSafeImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode Error { get; }

        public IImmutableList<VersionSupport> ApiVersions { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ApiVersionsResponse);
        }

        /// <inheritdoc />
        public bool Equals(ApiVersionsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error
                && ApiVersions.HasEqualElementsInOrder(other.ApiVersions);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (ApiVersions?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class VersionSupport : IEquatable<VersionSupport>
        {
            public override string ToString() => $"{{api_key:{ApiKey},min_version:{MinVersion},max_version:{MaxVersion}}}";

            public VersionSupport(ApiKey apiKey, short minVersion, short maxVersion)
            {
                ApiKey = apiKey;
                MinVersion = minVersion;
                MaxVersion = maxVersion;
            }

            /// <summary>
            /// API key.
            /// </summary>
            public ApiKey ApiKey { get; } 

            /// <summary>
            /// Minimum supported version.
            /// </summary>
            public short MinVersion { get; }

            /// <summary>
            /// Maximum supported version.
            /// </summary>
            public short MaxVersion { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as VersionSupport);
            }

            public bool Equals(VersionSupport other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ApiKey == other.ApiKey && MinVersion == other.MinVersion && MaxVersion == other.MaxVersion;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) ApiKey;
                    hashCode = (hashCode*397) ^ MinVersion.GetHashCode();
                    hashCode = (hashCode*397) ^ MaxVersion.GetHashCode();
                    return hashCode;
                }
            }

            #endregion

        }
    }
}