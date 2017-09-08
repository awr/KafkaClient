using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    internal class RoutedGroupRequest<T> : RoutedRequest<T> 
        where T : class, IResponse
    {
        public RoutedGroupRequest(IRequest<T> request, string groupId, ILog log)
            : base(request, log)
        {
            _groupId = groupId;
            var groupMember = request as IGroupMember;
            if (groupMember != null) {
                // on JoinGroupRequest, the first request is made with an empty string -- this still needs to go on a separate connection
                _memberId = groupMember is JoinGroupRequest && groupMember.MemberId == ""
                    ? Guid.NewGuid().ToString("")
                    : groupMember.MemberId;
            }
        }

        private readonly string _groupId;
        private readonly string _memberId;

        protected override async Task<IConnection> GetConnectionAsync(IRouter router, CancellationToken cancellationToken)
        {
            if (_memberId == null) {
                // can use any connection
                var groupConnection = await router.GetGroupConnectionAsync(_groupId, cancellationToken).ConfigureAwait(false);
                return groupConnection?.Connection;
            }
            return await router.GetConnectionAsync(_groupId, _memberId, cancellationToken);
        }

        protected override void ReturnConnection(IRouter router, IConnection connection)
        {
            if (_memberId != null) {
                router.ReturnConnection(_groupId, _memberId, connection);
            }
        }
    }
}