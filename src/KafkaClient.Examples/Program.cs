using System;
using CommandLineParser.Exceptions;

namespace KafkaClient.Examples
{
    public class Program
    {
        public static void Main(string[] args)
        {
            try {
                var parser = new CommandLineParser.CommandLineParser();
                var options = new ParsingTarget();
                parser.ExtractArgumentAttributes(options);
                parser.ParseCommandLine(args);

                IExample example;
                switch (options.Type) {
                    case "producer":
                        example = new Producer();
                        break;
                    case "consumer":
                        example = new SimpleConsumer();
                        break;
                    case "group":
                        example = new GroupConsumer();
                        break;

                    default:
                        throw new NotImplementedException($"unknown example {options.Type}");
                }

                Nito.AsyncEx.AsyncContext.Run(async () => await example.RunAsync(new KafkaOptions(new Uri(options.Server)), options.Topic));
            } catch (CommandLineException e) {
                Console.WriteLine(e.Message);
            }
        }
    }
}
