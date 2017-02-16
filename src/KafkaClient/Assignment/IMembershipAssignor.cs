using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Assignment
{
    /// <summary>
    /// Main extensibility point for the Group Consumer Leader to assign which member is assigned to which member(s).
    /// </summary>
    public interface IMembershipAssignor
    {
        string AssignmentStrategy { get; }

        Task<IImmutableDictionary<string, IMemberAssignment>> AssignMembersAsync(IRouter router, string groupId, int generationId, IImmutableDictionary<string, IMemberMetadata> memberMetadata, CancellationToken cancellationToken);
    }
}