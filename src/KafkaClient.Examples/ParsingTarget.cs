using CommandLineParser.Arguments;
#pragma warning disable 649

namespace KafkaClient.Examples
{
    class ParsingTarget
    {
        [ValueArgument(typeof(string), 't', "topic", Description = "Topic to run example with", DefaultValue = "ExampleTopic")]
        public string Topic;

        [ValueArgument(typeof(string), 's', "server", Description = "Kafka Server address", ValueOptional = false)]
        public string Server;

        [EnumeratedValueArgument(typeof(string), 'e', "example", Description = "Which example to run", DefaultValue = "producer", AllowedValues = "producer;consumer;group")]
        public string Type;
    }
}