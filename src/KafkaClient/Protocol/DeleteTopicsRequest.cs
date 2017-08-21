using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Request => [topics] timeout 
    /// </summary>
    /// <remarks>
    /// DeleteTopics Request => [topics] timeout 
    ///   topics => STRING
    ///   timeout => INT32
    ///
    /// From http://kafka.apache.org/protocol.html#The_Messages_DeleteTopics
    /// </remarks>
    public class DeleteTopicsRequest : Request, IRequest<DeleteTopicsResponse>, IEquatable<DeleteTopicsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{Topics.ToStrings()}],timeout:{Timeout}}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Topics, true)
                  .WriteMilliseconds(Timeout);
        }

        public DeleteTopicsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DeleteTopicsResponse.FromBytes(context, bytes);

        public DeleteTopicsRequest(params string[] topics)
            : this(topics, null)
        {
        }

        public DeleteTopicsRequest(IEnumerable<string> topics, TimeSpan? timeout = null)
            : base(ApiKey.DeleteTopics)
        {
            Topics = topics.ToSafeImmutableList();
            Timeout = timeout ?? TimeSpan.Zero;
        }

        /// <summary>
        /// The topics to be deleted.
        /// </summary>
        public IImmutableList<string> Topics { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely deleted on the controller node. 
        /// Values &lt;= 0 will trigger topic deletion and return immediately
        /// </summary>
        public TimeSpan Timeout { get; }

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
                && Topics.HasEqualElementsInOrder(other.Topics)
                && Timeout.Equals(other.Timeout);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Timeout.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}