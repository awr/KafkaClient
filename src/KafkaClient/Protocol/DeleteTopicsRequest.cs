using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Request => [topics] timeout 
    ///  topics => STRING -- An array of topics to be deleted.
    ///  timeout => INT32 -- The time in ms to wait for a topic to be completely deleted on the controller node. Values &lt;= 0 will trigger topic deletion and return immediately
    /// </summary>
    public class DeleteTopicsRequest : Request, IRequest<DeleteTopicsResponse>, IEquatable<DeleteTopicsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{topics.ToStrings()}],timeout:{timeout}}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {topics[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(topics, true)
                  .Write((int) timeout.TotalMilliseconds);
        }

        public DeleteTopicsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DeleteTopicsResponse.FromBytes(context, bytes);

        public DeleteTopicsRequest(params string[] topics)
            : this(topics, null)
        {
        }

        public DeleteTopicsRequest(IEnumerable<string> topics, TimeSpan? timeout = null)
            : base(ApiKey.DeleteTopics)
        {
            this.topics = ImmutableList<string>.Empty.AddNotNullRange(topics);
            this.timeout = timeout ?? TimeSpan.Zero;
        }

        public IImmutableList<string> topics { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely deleted on the controller node. 
        /// Values &lt;= 0 will trigger topic deletion and return immediately
        /// </summary>
        public TimeSpan timeout { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteTopicsRequest);
        }

        public bool Equals(DeleteTopicsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && topics.HasEqualElementsInOrder(other.topics)
                && timeout.Equals(other.timeout);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (topics?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ timeout.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}