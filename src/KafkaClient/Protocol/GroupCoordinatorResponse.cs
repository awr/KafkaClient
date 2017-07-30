using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// GroupCoordinator Response => error_code coordinator
    ///  error_code => INT16 -- The error code.
    ///  coordinator => node_id host port 
    ///   node_id => INT32   -- The broker id.
    ///   host => STRING     -- The hostname of the broker.
    ///   port => INT32      -- The port on which the broker accepts requests.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class GroupCoordinatorResponse : Server, IResponse, IEquatable<GroupCoordinatorResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},node_id:{Id},host:'{Host}',port:{Port}}}";

        public static GroupCoordinatorResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var coordinatorId = reader.ReadInt32();
                var coordinatorHost = reader.ReadString();
                var coordinatorPort = reader.ReadInt32();

                return new GroupCoordinatorResponse(errorCode, coordinatorId, coordinatorHost, coordinatorPort);
            }
        }

        public GroupCoordinatorResponse(ErrorCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorCode error_code { get; }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupCoordinatorResponse);
        }

        /// <inheritdoc />
        public bool Equals(GroupCoordinatorResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && error_code == other.error_code;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (int) error_code;
            }
        }
    }
}