using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ApiVersions Request => 
    /// 
    /// A Protocol for requesting which versions are supported for each api key
    /// </summary>
    /// <remarks>
    /// ApiVersions Request => 
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_ApiVersions
    /// </remarks>
    public class ApiVersionsRequest : Request, IRequest<ApiVersionsResponse>, IEquatable<ApiVersionsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey}}}";

        public ApiVersionsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => ApiVersionsResponse.FromBytes(context, bytes);

        public ApiVersionsRequest() 
            : base(ApiKey.ApiVersions)
        {
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ApiVersionsRequest);
        }

        public bool Equals(ApiVersionsRequest other)
        {
            return base.Equals(other);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}