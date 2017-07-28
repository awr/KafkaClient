using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    /// <summary>
    /// An Assignor whose first priority is to be sticky to previous assignments, even if this leaves some (new) members without assignments.
    /// </summary>
    public class SimpleAssignor : MembershipAssignor<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        public static ImmutableList<IMembershipAssignor> Assignors { get; } = ImmutableList<IMembershipAssignor>.Empty.Add(new SimpleAssignor());

        public const string Strategy = "sticky";

        public SimpleAssignor() : base(Strategy)
        {
        }

        protected override async Task<IImmutableDictionary<string, ConsumerMemberAssignment>> AssignAsync(
            IRouter router, string groupId, int generationId, IImmutableDictionary<string, ConsumerProtocolMetadata> memberMetadata,
            CancellationToken cancellationToken)
        {
            var topicNames = memberMetadata.Values.SelectMany(m => m.Subscriptions).Distinct().ToList();
            var topicMetadata = await router.GetTopicMetadataAsync(topicNames, cancellationToken).ConfigureAwait(false);
            var partitions = new HashSet<TopicPartition>(topicMetadata.SelectMany(t => t.partition_metadata.Select(p => new TopicPartition(t.topic, p.partition_id))));

            var previousAssignments = router.GetGroupMemberAssignment(groupId); // take the latest we've got, ignoring whether it's a current gen
            var assignments = memberMetadata.Keys.ToDictionary(_ => _, _ => new List<TopicPartition>());
            foreach (var currentAssignment in previousAssignments.Where(a => a.Value != null)) {
                foreach (var partition in currentAssignment.Value.PartitionAssignments) {
                    if (partitions.Contains(partition) && assignments.TryGetValue(currentAssignment.Key, out List<TopicPartition> assignment)) {
                        assignment.Add(partition);
                        partitions.Remove(partition);
                    }
                }
            }

            var memberIds = assignments.OrderBy(a => a.Value.Count).Select(a => a.Key).ToImmutableArray();
            var memberIndex = 0;
            foreach (var partition in partitions.OrderBy(_ => _.PartitionId)) {
                 var memberId = memberIds[memberIndex++ % memberIds.Length];
                if (assignments[memberId].Count == 0) {
                    assignments[memberId].Add(partition);
                }
            }

            return assignments.ToImmutableDictionary(pair => pair.Key, pair => new ConsumerMemberAssignment(pair.Value));
        }
    }
}