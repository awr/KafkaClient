using System;
using Xunit.Sdk;

namespace KafkaClient.Tests
{
    [TraitDiscoverer("FlakyDiscoverer", "TraitExtensibility")]
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public class FlakyAttribute : Attribute, ITraitAttribute
    {
    }
}