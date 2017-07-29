using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    /// <summary>
    /// Provides a mechanism for fetching batches of messages, possibly from many topics and partitions. 
    /// This implementation can be used for group consumption, enabling complex coordination across multiple consumers of a set of 
    /// Kafka topics. Group consumption is where consumer assignment comes into play, through the configured 
    /// <see cref="IMembershipAssignor"/>.
    /// There are many extensions available through <see cref="Extensions"/>, making consumption simpler.
    /// </summary>
    public class GroupConsumer : IGroupConsumer
    {
        private readonly bool _leaveRouterOpen;
        private IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        public GroupConsumer(IRouter router, string groupId, string protocolType, JoinGroupResponse response, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool? leaveRouterOpen = null, bool? autoConsume = null)
        {
            Router = router;
            _leaveRouterOpen = leaveRouterOpen.GetValueOrDefault(true);
            AutoConsume = autoConsume.GetValueOrDefault(false);
            Configuration = configuration ?? ConsumerConfiguration.Default;
            Encoders = encoders ?? ConnectionConfiguration.Defaults.Encoders();

            if (!Encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(protocolType), $"ProtocolType {protocolType} is unknown");

            GroupId = groupId;
            MemberId = response.member_id;
            ProtocolType = protocolType;

            OnJoinGroup(response);

            // Attempt to send heartbeats at half intervals to better ensure we don't miss the session timeout deadline
            // TODO: should this be something like Math.Min(Configuration.GroupHeartbeat.TotalMilliseconds, Configuration.GroupRebalanceTimeout.TotalMilliseconds) instead?
            _heartbeatDelay = TimeSpan.FromMilliseconds(Configuration.GroupHeartbeat.TotalMilliseconds / 2);
            _heartbeatTask = Task.Factory.StartNew(DedicatedHeartbeatAsync, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            _stateChangeQueue = new AsyncProducerConsumerQueue<ApiKey>();
            _stateChangeTask = Task.Factory.StartNew(DedicatedStateChangeAsync, _disposeToken.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount; // = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        private bool _leaveOnDispose = true;

        private int _activeHeartbeatCount;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatDelay;

        private int _activeStateChangeCount;
        private readonly Task _stateChangeTask;
        private readonly AsyncProducerConsumerQueue<ApiKey> _stateChangeQueue;

        private readonly SemaphoreSlim _joinSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _syncSemaphore = new SemaphoreSlim(1, 1);
        private int _syncCount; // used with _fetchSemaphore as a wait until sync has completed for the first time
        private readonly SemaphoreSlim _fetchSemaphore = new SemaphoreSlim(0, 1);
        private ImmutableDictionary<string, IMemberMetadata> _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty;
        private IImmutableDictionary<TopicPartition, IMessageBatch> _batches = ImmutableDictionary<TopicPartition, IMessageBatch>.Empty;
        private IMemberAssignment _assignment;

        public bool IsLeader { get; private set; }
        public int GenerationId { get; private set; }
        private string _groupProtocol;

        public string GroupId { get; }
        public string MemberId { get; }

        public IRouter Router { get; }
        public IConsumerConfiguration Configuration { get; }
        public bool AutoConsume { get; }

        /// <summary>
        /// State machine for Member state
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for basis
        /// </summary>
        /// <remarks>
        ///                            +===========+
        ///                            [           ]
        ///     +----------------------+  Assign   ]
        ///     |                      [           ]
        ///     |                      +=====+=====+
        ///     |                            ^
        ///     | SyncGroupRequest           | JoinGroupResponse
        ///     | (only leader assigns)      | ResponseCode.None
        ///     |                            |
        ///     |                      +-----+-----+
        ///     |                      |           |
        ///     |                  +--->  Joining  |
        ///     |                  |   |           |
        ///     |                  |   +-----+-----+
        ///     |                  |         |
        ///     |        JoinGroup |         | JoinGroupResponse
        ///     |        Request   |         | ResponseCode.GroupCoordinatorNotAvailable
        ///     |                  |         | ResponseCode.GroupLoadInProgress
        ///     |                  |         v                 
        ///     |                  |   +-----+-----+
        ///     |                  +---+           |
        ///     |                      |  Rejoin   |
        ///     |  +------------------->           |
        ///     |  | SyncGroupResponse +-----+-----+
        ///     v  | RebalanceInProgress     ^
        ///  +--+--+-----+                   | HeartbeatResponse
        ///  |           |                   | ResponseCode.RebalanceInProgress
        ///  |  Syncing  |                   |
        ///  |           |             +-----+--------+
        ///  +-----+-----+             |              |
        ///        |               +---> Heartbeating |
        ///        |               |   |              |
        ///        |               |   +-----+--------+
        ///        |               |         |
        ///        |     Heartbeat |         | HeartbeatResponse
        ///        |     Request   |         | ResponseCode.None
        ///        |               |         v
        ///        |               |   +-----+-----+
        ///        |               +---+           |
        ///        |                   |  Stable   |
        ///        +------------------->           |
        ///        SyncGroupResponse   +-----------+ 
        ///        ResponseCode.None                   
        /// </remarks>
        private async Task DedicatedHeartbeatAsync()
        {
            // only allow one heartbeat to execute, dump out all other requests
            if (Interlocked.Increment(ref _activeHeartbeatCount) != 1) return;

            try {
                Router.Log.Info(() => LogEvent.Create($"Starting heartbeat for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
                var delay = _heartbeatDelay;
                while (!_disposeToken.IsCancellationRequested) {
                    try {
                        await Task.Delay(delay, _disposeToken.Token).ConfigureAwait(false);
                        await Router.SendAsync(new HeartbeatRequest(GroupId, GenerationId, MemberId), GroupId, _disposeToken.Token, retryPolicy: Configuration.GroupCoordinationRetry).ConfigureAwait(false);
                        delay = _heartbeatDelay;
                    } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
                    } catch (RequestException ex) {
                        switch (ex.ErrorCode) {
                            case ErrorCode.REBALANCE_IN_PROGRESS:
                                Router.Log.Info(() => LogEvent.Create(ex.Message));
                                TriggerRejoin();
                                delay = _heartbeatDelay;
                                break;

                            case ErrorCode.GROUP_AUTHORIZATION_FAILED:
                            case ErrorCode.UNKNOWN_MEMBER_ID:
                                Router.Log.Warn(() => LogEvent.Create(ex));
                                _leaveOnDispose = false; // no point in attempting to leave the group since it will fail
                                _disposeToken.Cancel();
                                break;

                            default:
                                Router.Log.Info(() => LogEvent.Create(ex));
                                if (ex.ErrorCode.IsRetryable()) {
                                    delay = TimeSpan.FromMilliseconds(Math.Max(delay.TotalMilliseconds / 2, 1000));
                                }
                                break;
                        }
                    } catch (Exception ex) {
                        Router.Log.Warn(() => LogEvent.Create(ex));
                        HandleDispose(ex as ObjectDisposedException);
                    }
                }
            } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
            } catch (Exception ex) {
                Router.Log.Warn(() => LogEvent.Create(ex));
            } finally {
                await DisposeAsync().ConfigureAwait(false); // safe to call in multiple places
                Interlocked.Decrement(ref _activeHeartbeatCount);
                Router.Log.Info(() => LogEvent.Create($"Stopped heartbeat for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
            }
        }

        /// <summary>
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for details
        /// </summary>
        private async Task DedicatedStateChangeAsync()
        {
            // only allow one heartbeat to execute, dump out all other requests
            if (Interlocked.Increment(ref _activeStateChangeCount) != 1) return;

            try {
                Router.Log.Info(() => LogEvent.Create($"Starting state change for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
                ApiKey? nextRequest = ApiKey.SyncGroup;
                var failures = 0;
                while (!_disposeToken.IsCancellationRequested) {
                    try {
                        if (!nextRequest.HasValue) {
                            var next = await _stateChangeQueue.DequeueAsync(_disposeToken.Token);
                            Router.Log.Info(() => LogEvent.Create($"Triggered {next} for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
                            nextRequest = next;
                            failures = 0;
                        }

                        var apiKey = nextRequest.Value;
                        switch (apiKey) {
                            case ApiKey.JoinGroup:
                                await JoinGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                break;

                            case ApiKey.SyncGroup:
                                await SyncGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                break;

                            default:
                                Router.Log.Warn(() => LogEvent.Create($"Ignoring unknown state change {apiKey} for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
                                break;
                        }
                        nextRequest = null;
                    } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
                    } catch (RequestException ex) {
                        Router.Log.Info(() => LogEvent.Create(ex));
                        await Task.Delay(Configuration.GroupCoordinationRetry.RetryDelay(failures++, TimeSpan.Zero) ?? TimeSpan.FromSeconds(1)); // avoid spamming, but do retry same request
                    } catch (Exception ex) {
                        Router.Log.Warn(() => LogEvent.Create(ex));
                        HandleDispose(ex as ObjectDisposedException);
                    }
                }
            } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
            } catch (Exception ex) {
                Router.Log.Warn(() => LogEvent.Create(ex));
            } finally {
                await DisposeAsync().ConfigureAwait(false); // safe to call in multiple places
                Interlocked.Decrement(ref _activeStateChangeCount);
                Router.Log.Info(() => LogEvent.Create($"Stopped state change for {{GroupId:{GroupId},MemberId:{MemberId}}}"));
            }
        }

        private void HandleDispose(ObjectDisposedException exception)
        {
            if (exception?.ObjectName == nameof(Router)) {
                _leaveOnDispose = false; // no point in attempting to leave the group since it will fail
                _disposeToken.Cancel();
            }
        }

        private void TriggerRejoin()
        {
            try {
                _stateChangeQueue.Enqueue(ApiKey.JoinGroup, _disposeToken.Token);
            } catch (Exception ex) {
                if (_disposeCount == 0) {
                    Router.Log.Warn(() => LogEvent.Create(ex));
                }
            }
        }

        private async Task JoinGroupAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException($"Consumer {{GroupId:{GroupId},MemberId:{MemberId}}} is no longer valid");

            try {
                var protocols = _joinSemaphore.Lock(() => IsLeader ? _memberMetadata?.Values.Select(m => new JoinGroupRequest.GroupProtocol(m)) : null, cancellationToken);
                var request = new JoinGroupRequest(GroupId, Configuration.GroupHeartbeat, MemberId, ProtocolType, protocols, Configuration.GroupRebalanceTimeout);
                var response = await Router.SendAsync(request, GroupId, cancellationToken, new RequestContext(protocolType: ProtocolType), Configuration.GroupCoordinationRetry).ConfigureAwait(false);
                OnJoinGroup(response);
                await _stateChangeQueue.EnqueueAsync(ApiKey.SyncGroup, _disposeToken.Token);
            } catch (RequestException ex) {
                switch (ex.ErrorCode) {
                    case ErrorCode.ILLEGAL_GENERATION:
                    case ErrorCode.GROUP_AUTHORIZATION_FAILED:
                    case ErrorCode.UNKNOWN_MEMBER_ID:
                    case ErrorCode.INCONSISTENT_GROUP_PROTOCOL:
                    case ErrorCode.INVALID_SESSION_TIMEOUT:
                        Router.Log.Warn(() => LogEvent.Create(ex));
                        _leaveOnDispose = false; // no point in attempting to leave the group since it will fail
                        _disposeToken.Cancel();
                        return;
                }
                throw;
            }
        }

        /// <summary>
        /// Called from the consumer when joining/rejoining..
        /// 
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
        /// </summary>
        private void OnJoinGroup(JoinGroupResponse response)
        {
            if (response.member_id != MemberId) throw new ArgumentOutOfRangeException(nameof(response), $"Member is not valid ({MemberId} != {response.member_id})");
            if (_disposeCount > 0) throw new ObjectDisposedException($"Consumer {{GroupId:{GroupId},MemberId:{MemberId}}} is no longer valid");

            _joinSemaphore.Lock(
                () => {
                    IsLeader = response.leader_id == MemberId;
                    GenerationId = response.generation_id;
                    _groupProtocol = response.group_protocol;
                    _memberMetadata = response.members.ToImmutableDictionary(member => member.member_id, member => member.member_metadata);
                    Router.Log.Info(() => LogEvent.Create(GenerationId > 1 
                        ? $"Consumer {MemberId} Rejoined {GroupId} Generation{GenerationId}"
                        : $"Consumer {MemberId} Joined {GroupId}"));
                }, _disposeToken.Token);
        }

        public async Task SyncGroupAsync(CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException($"Consumer {{GroupId:{GroupId},MemberId:{MemberId}}} is no longer valid");

            var groupAssignments = await _joinSemaphore.LockAsync(
                async () => {
                    if (IsLeader) {
                        var encoder = Encoders[ProtocolType];
                        var assigner = encoder.GetAssignor(_groupProtocol);
                        var assignments = await assigner.AssignMembersAsync(Router, GroupId, GenerationId, _memberMetadata, cancellationToken).ConfigureAwait(false);
                        return assignments.Select(pair => new SyncGroupRequest.GroupAssignment(pair.Key, pair.Value));
                    }
                    return null;
                }, _disposeToken.Token).ConfigureAwait(false);
            await Task.WhenAll(_batches.Values.Select(b => b.CommitMarkedIgnoringDisposedAsync(cancellationToken))).ConfigureAwait(false);

            SyncGroupResponse response;
            try {
                var request = new SyncGroupRequest(GroupId, GenerationId, MemberId, groupAssignments);
                response = await Router.SyncGroupAsync(request, new RequestContext(protocolType: ProtocolType), Configuration.GroupCoordinationRetry, cancellationToken).ConfigureAwait(false);
            } catch (RequestException ex) {
                switch (ex.ErrorCode) {
                    case ErrorCode.REBALANCE_IN_PROGRESS:
                        Router.Log.Info(() => LogEvent.Create(ex.Message));
                        TriggerRejoin();
                        return;

                    case ErrorCode.GROUP_AUTHORIZATION_FAILED:
                    case ErrorCode.UNKNOWN_MEMBER_ID:
                        Router.Log.Warn(() => LogEvent.Create(ex));
                        _leaveOnDispose = false; // no point in attempting to leave the group since it will fail
                        _disposeToken.Cancel();
                        break;
                }
                throw;
            }

            _syncSemaphore.Lock(() => {
                _assignment = response.member_assignment;
                var validPartitions = response.member_assignment.PartitionAssignments.ToImmutableHashSet();
                var invalidPartitions = _batches.Where(pair => !validPartitions.Contains(pair.Key)).ToList();
                foreach (var invalidPartition in invalidPartitions) {
                    invalidPartition.Value.Dispose();
                }
                _batches = _batches.RemoveRange(invalidPartitions.Select(pair => pair.Key));
            }, _disposeToken.Token);

            if (Interlocked.Increment(ref _syncCount) == 1) {
                _fetchSemaphore.Release();
            }
        }

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                Router.Log.Debug(() => LogEvent.Create($"Disposing Consumer {{GroupId:{GroupId},MemberId:{MemberId}}}"));
                _disposeToken.Cancel();
                _fetchSemaphore.Dispose();
                _joinSemaphore.Dispose();
                _syncSemaphore.Dispose();

                try {
                    var batches = Interlocked.Exchange(ref _batches, ImmutableDictionary<TopicPartition, IMessageBatch>.Empty);
                    await Task.WhenAll(batches.Values.Select(b => b.CommitMarkedAsync(CancellationToken.None))).ConfigureAwait(false);
                    foreach (var batch in batches.Values) {
                        batch.Dispose();
                    }
                } catch (Exception ex) {
                    Router.Log.Info(() => LogEvent.Create(ex));
                }
                _assignment = null;

                try {
                    await Task.WhenAny(_heartbeatTask, _stateChangeTask, Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None)).ConfigureAwait(false);
                    if (_leaveOnDispose) {
                        var request = new LeaveGroupRequest(GroupId, MemberId);
                        await Router.SendAsync(request, GroupId, CancellationToken.None, retryPolicy: Retry.None).ConfigureAwait(false);
                    }
                } catch (Exception ex) {
                    Router.Log.Info(() => LogEvent.Create(ex));
                }

                if (!_leaveRouterOpen) {
                    await Router.DisposeAsync();
                }
                _disposeToken.Dispose();
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        /// <summary>
        /// Leave the consumer group and stop heartbeats.
        /// </summary>
        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        public string ProtocolType { get; }

        /// <summary>
        /// Fetch messages for this consumer group's current assignment.
        /// Messages are collected together in a batch per assignment. Each batch can be used to get available messages, 
        /// commit offsets and get subsequent batches on the given topic/partition. Once the topic/partition is reassigned, the batch will be disposed.
        /// 
        /// Subsequent calls to this function will result in new batches for each assignment. Once all active assignments have been given,
        /// the <see cref="MessageBatch.Empty"/> result will be used as an indication of nothing being currently available.
        /// </summary>
        public async Task<IMessageBatch> FetchAsync(CancellationToken cancellationToken, int? batchSize = null)
        {
            return await _fetchSemaphore.LockAsync(
                async () => {
                    if (_disposeCount > 0) throw new ObjectDisposedException($"Consumer {{GroupId:{GroupId},MemberId:{MemberId}}} is no longer valid");

                    var generationId = GenerationId;
                    var partition = _syncSemaphore.Lock(() => _assignment?.PartitionAssignments.FirstOrDefault(p => !_batches.ContainsKey(p)), _disposeToken.Token);

                    if (partition == null) return MessageBatch.Empty;
                    var currentOffset = await Router.GetOffsetsAsync(GroupId, partition.TopicName, partition.PartitionId, cancellationToken).ConfigureAwait(false);
                    var offset = currentOffset.Offset + 1;
                    var messages = await Router.FetchMessagesAsync(ImmutableList<Message>.Empty, partition.TopicName, partition.PartitionId, offset, Configuration, cancellationToken, batchSize).ConfigureAwait(false);
                    var batch = new MessageBatch(messages, partition, offset, Router, Configuration, AutoConsume, batchSize, GroupId, MemberId, generationId);
                    _syncSemaphore.Lock(() => _batches = _batches.Add(partition, batch), cancellationToken);
                    return batch;                    
                }, cancellationToken).ConfigureAwait(false);
        }
    }
}