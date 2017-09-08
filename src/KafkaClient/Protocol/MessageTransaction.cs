using System.Collections.Generic;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    public class MessageTransaction
    {
        public MessageTransaction(IEnumerable<Message> messages, ITransactionContext transaction = null)
        {
            Messages = messages.ToImmutableList();
            Transaction = transaction;
        }

        public IImmutableList<Message> Messages { get; }

        public ITransactionContext Transaction { get; }
    }
}