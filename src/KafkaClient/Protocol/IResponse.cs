using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    public interface IResponse
    {
        /// <summary>
        /// Any errors from the server
        /// </summary>
        IImmutableList<ErrorCode> Errors { get; }
    }

    public interface IResponse<T> : IResponse
    {
        // ReSharper disable once InconsistentNaming
        IImmutableList<T> Responses { get; }
    }
}