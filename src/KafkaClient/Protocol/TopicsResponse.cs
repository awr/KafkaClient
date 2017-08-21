using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public abstract class TopicsResponse : ThrottledResponse, IResponse, IEquatable<TopicsResponse>
    {
        public override string ToString() => $"{{Topics:[{Topics.ToStrings()}]}}";

        protected TopicsResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Topics = topics.ToSafeImmutableList();
            Errors = Topics.Select(t => t.ErrorCode).ToImmutableList();
        }

        public IImmutableList<Topic> Topics { get; } 
        public IImmutableList<ErrorCode> Errors { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicsResponse);
        }

        public bool Equals(TopicsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{TopicName:{TopicName},ErrorCode:{ErrorCode}}}";

            public Topic(string topicName, ErrorCode errorCode, string errorMessage = null)
            {
                TopicName = topicName;
                ErrorCode = errorCode;
                ErrorMessage = errorMessage;
            }

            public string TopicName { get; }
            public ErrorCode ErrorCode { get; }
            public string ErrorMessage { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(TopicName, other.TopicName) 
                    && string.Equals(ErrorMessage, other.ErrorMessage) 
                    && ErrorCode == other.ErrorCode;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = TopicName?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ ErrorCode.GetHashCode();
                    hashCode = (hashCode * 397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}