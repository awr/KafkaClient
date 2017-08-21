using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Metadata Request => [topics] *allow_auto_topic_creation 
    /// </summary>
    /// <remarks>
    /// Metadata Request => [topics] *allow_auto_topic_creation 
    ///   topics => STRING
    ///   allow_auto_topic_creation => BOOLEAN
    /// 
    /// Version 4+: allow_auto_topic_creation
    /// From http://kafka.apache.org/protocol.html#The_Messages_Metadata
    /// </remarks>
    public class MetadataRequest : Request, IRequest<MetadataResponse>, IEquatable<MetadataRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Topics, true);
            if (context.ApiVersion >= 4) {
                writer.Write(AllowTopicAutoCreation.GetValueOrDefault());
            }
        }

        public MetadataResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => MetadataResponse.FromBytes(context, bytes);

        public MetadataRequest(string topic, bool? allowTopicAutoCreation = null)
            : this (new []{topic}, allowTopicAutoCreation)
        {
        }

        public MetadataRequest(IEnumerable<string> topics = null, bool? allowTopicAutoCreation = null) 
            : base(ApiKey.Metadata)
        {
            Topics = topics.ToSafeImmutableList();
            AllowTopicAutoCreation = allowTopicAutoCreation;
        }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public IImmutableList<string> Topics { get; }

        /// <summary>
        /// If this and the broker config 'auto.create.topics.enable' are true, topics that don't exist will be created by the broker. 
        /// Otherwise, no topics will be created by the broker.
        /// Version: 4+
        /// </summary>
        public bool? AllowTopicAutoCreation { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataRequest);
        }

        /// <inheritdoc />
        public bool Equals(MetadataRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && Topics.HasEqualElementsInOrder(other.Topics)
                && AllowTopicAutoCreation == other.AllowTopicAutoCreation;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (AllowTopicAutoCreation?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion
    }
}