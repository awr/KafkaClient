using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ApiVersions Response => error_code [api_versions]
    ///  error_code => INT16   -- The error code.
    ///  api_version => api_key min_version max_version 
    ///   api_key => INT16     -- The Api Key.
    ///   min_version => INT16 -- The minimum supported version.
    ///   max_version => INT16 -- The maximum supported version.
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class ApiVersionsResponse : IResponse, IEquatable<ApiVersionsResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},api_versions:[{api_versions.ToStrings()}]}}";

        public static ApiVersionsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                var apiKeys = new VersionSupport[reader.ReadInt32()];
                for (var i = 0; i < apiKeys.Length; i++) {
                    var apiKey = (ApiKey)reader.ReadInt16();
                    var minVersion = reader.ReadInt16();
                    var maxVersion = reader.ReadInt16();
                    apiKeys[i] = new VersionSupport(apiKey, minVersion, maxVersion);
                }
                return new ApiVersionsResponse(errorCode, apiKeys);
            }
        }

        public ApiVersionsResponse(ErrorCode errorCode = ErrorCode.NONE, IEnumerable<VersionSupport> supportedVersions = null)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
            api_versions = ImmutableList<VersionSupport>.Empty.AddNotNullRange(supportedVersions);
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// The error code.
        /// </summary>
        public ErrorCode error_code { get; }

        public IImmutableList<VersionSupport> api_versions { get; }

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
            return error_code == other.error_code
                && api_versions.HasEqualElementsInOrder(other.api_versions);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) error_code*397) ^ (api_versions?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class VersionSupport : IEquatable<VersionSupport>
        {
            public override string ToString() => $"{{api_key:{api_key},min_version:{min_version},max_version:{max_version}}}";

            public VersionSupport(ApiKey apiKey, short minVersion, short maxVersion)
            {
                api_key = apiKey;
                min_version = minVersion;
                max_version = maxVersion;
            }

            /// <summary>
            /// API key.
            /// </summary>
            public ApiKey api_key { get; } 

            /// <summary>
            /// Minimum supported version.
            /// </summary>
            public short min_version { get; }

            /// <summary>
            /// Maximum supported version.
            /// </summary>
            public short max_version { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as VersionSupport);
            }

            public bool Equals(VersionSupport other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return api_key == other.api_key && min_version == other.min_version && max_version == other.max_version;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) api_key;
                    hashCode = (hashCode*397) ^ min_version.GetHashCode();
                    hashCode = (hashCode*397) ^ max_version.GetHashCode();
                    return hashCode;
                }
            }

            #endregion

        }
    }
}