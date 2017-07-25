# KafkaClient
Native async C# client for Apache Kafka, supporting versions [0.9, 0.10.1]

## Status

| OS      | Framework | Status |
|---------|-----------|--------|
| Windows | .net 4.6.2 | [![Build status](https://ci.appveyor.com/api/projects/status/54mgnutld37dpn9a?svg=true)](https://ci.appveyor.com/project/AndrewRobinson/kafkaclient) [![codecov](https://codecov.io/gh/awr/KafkaClient/branch/master/graph/badge.svg)](https://codecov.io/gh/awr/KafkaClient) |
| Windows | .net standard 1.6 | [![Build status](https://ci.appveyor.com/api/projects/status/e7ej2g9q77if8mkf?svg=true)](https://ci.appveyor.com/project/AndrewRobinson/kafkanetclient) |
| Linux   | .net standard 1.6 | [![Build status](https://api.travis-ci.org/awr/KafkaClient.svg?branch=master)](https://travis-ci.org/awr/KafkaClient) |

[![NuGet Badge](https://buildstats.info/nuget/KafkaClient)](https://www.nuget.org/packages/KafkaClient/)

###  *** WARNING ***
This library is still work in progress and has not yet been deployed to production. It is also undergoing significant development, and breaking changes will occour.
This notice will be removed once it's been stabilized and used in production.

The biggest missing piece at this point is [stress testing](https://github.com/awr/KafkaClient/issues/17); more [comprehensive automated tests](https://github.com/awr/KafkaClient/issues/18) are also needed. For the full set, see the [backlog](https://github.com/awr/KafkaClient/projects/1).

## License
Copyright 2016, Nudge Software Inc under Apache License, V2.0. See LICENSE file.

## Code Examples
#### Producer
```csharp
var options = new KafkaOptions(new Uri("tcp://SERVER1:9092"), new Uri("tcp://SERVER2:9092"));

// multiple calls may batch into a single tcp request, depending on configured options
using (var producer = await options.CreateProducerAsync()) {
	// single message
	await producer.SendAsync(new Message("hello world"), "TopicName", CancellationToken.None);

	// batch 
	var messages = Enumerable.Range(0, 100).Select(i => new Message($"Value {i}", i.ToString()));
	await producer.SendAsync(messages, "TopicName", CancellationToken.None);
}
```

#### Consumer (Simple)
```csharp
var options = new KafkaOptions(new Uri("tcp://SERVER1:9092"), new Uri("tcp://SERVER2:9092"));

using (var consumer = await options.CreateConsumerAsync("TopicName", 0)) {
	await consumer.ConsumeAsync(
		message => Console.WriteLine($"TopicName: {message.Value.ToString()}"),
		cancellationToken); // consuming ends when cancellation is triggered
}
```

#### Consumer (Groups)
```csharp
var options = new KafkaOptions(new Uri("tcp://SERVER1:9092"), new Uri("tcp://SERVER2:9092"));

var groupId = "GroupName";
var topicName = "TopicName";
var metadata = new ConsumerProtocolMetadata(topicName);
using (var consumer = await options.CreateGroupConsumerAsync(groupId, metadata, cancellationToken)) {
	// async and batch overloads also available
	await consumer.ConsumeAsync(
		message => Console.WriteLine($"{topicName}: {message.Value.ToString()}"),
		cancellationToken); // consuming ends when cancellation is triggered
}
```

Low-level access is possible, directly through the Router.
For more examples, see [the Examples Project](https://github.com/awr/KafkaClient/tree/master/src/KafkaClient.Examples).

## Configuration
Top level configuration is contained in `KafkaClient.KafkaOptions`, which has the `ILog` and the initial server connections. Otherwise, this really acts to coordinate the separate configurations of each part of the client.
- Producer batching and timeouts are managed through `KafkaClient.IProducerConfiguration`.
  - Sending codec and server acks are managed through `KafkaClient.ISendMessageConfiguration`.
  - Partition selection is done through `KafkaClient.IPartitionSelector`.
- Consumer timeouts and retries are managed through `KafkaClient.IConsumerConfiguration`, including minimum and maximum byte sizes to retrieve from the Kafka servers on each request.
- Connection and request details are managed through `KafkaClient.Connections.IConnectionConfiguration`, including retry and timeouts for the connection and requests. 
  - Assignment encoding, versioning and ssl support is configured here. 
  - Telemetry is possible by attaching to events exposed by the Connection configuration.
- Server and topic/group metadata caching is managed through `KafkaClient.IRouterConfiguration`.

All configuration implementations are immutable.

## Key Concepts
### Connection 
A `KafkaClient.Connections.IConnection` provides async methods to send data to a kafka server. It uses a persistent connection, and interleaves requests and responses. The send method internally uses the `KafkaClient.Connections.ITransport` abstraction to allow for either direct tcp socket access, or ssl stream access (when ssl is configured). Tcp reconnection is coordinated between the `IConnection` and the `ITransport`, based on the configuration settings for ssl.

The connection is rarely used directly, since it is tied to a particular kafka server. Low level requests at the protocol level are better done through the `KafkaClient.IRouter`.

### Producer
A `KafkaClient.IProducer` provides a high level abstraction for sending messages (batches or otherwise) to a Kafka cluster.

There are several extensions available through `KafkaClient.Extensions`, making production simpler. In particular, it is possible to select the partition based on the topic metadata and message key by way of a `KafkaClient.IPartitionSelector`.

#### Partition Selection
Provides the logic for routing requests to a particular topic to a partition. The default selector (`KafkaClient.PartitionSelector`) will use round robin partition selection if the key property on the message is null and a mod/hash of the key value if present.

### Consumer
The `KafkaClient.IConsumer` provides a mechanism for fetching batches of messages, possibly from many topics and partitions. 

There are many extensions available through `KafkaClient.Extensions`, making consumption simpler.

The `KafkaClient.Consumer` implementation can be used for simple consumption, and is useful for going back to previous messages or managing data.
The `KafkaClient.GroupConsumer` implementation can be used for group consumption, enabling complex coordination across multiple consumers of a set of Kafka topics. Group consumption is where consumer assignment comes into play, through the configured `KafkaClient.Assignment.IMembershipAssignor`.

#### Message Batches
The `KafkaClient.IMessageBatch` is used to consume messages sequentially on a single topic and partition. Messages can be marked as completed (for group consumers especially), and subsequent `KafkaClient.IMessageBatch`es can be retrieved.

When using the more idiomatic approach of one-message-at-a-time receiving, there is no need to interact with the batch directly. However, all consumption uses batches under the hood for performance.

#### Consumer Assignment
The `KafkaClient.Assignment.IMembershipAssignor` approach enables extensibility for group member assignment. 

The default assignor (`KafkaClient.Assignment.SimpleAssignor`) will round robin partition selection across topics, while attempting to to be sticky to previous assignments. 

### Request Routing
The `KafkaClient.IRouter` provides routing for kafka servers and connections, based on the topic and partition or group the kafka request is related to. It also provides caching for the metadata around topic and group metadata, and management of connections.

Low level requests (at the protocol level) make most sense with a router, which is why it's part of `KafkaClient.Assignment.AssignMembersAsync()`. 

Finally, since consumer group management requests (for the same groupId) must be done on unique connections, the router also manages the selection of connection for group management requests.

#### Binary Protocol
The protocol has been divided up into concrete classes for each request/response pair, in `KafkaClient.Protocol`. Each request/response object knows how to encode or decode (respectively), as well as produce useful documentation for logging. Additional context for encoding and decoding (such as the correlation id) is provided through the `KafkaClient.Protocol.IRequestContext`.

History
-----------
This client is a fork of [gigya]'s [KafkaNetClient], itself a fork of [jroland]'s [kafka-net] client, which was based on [David Arthur]'s [kafka-python] client. Thanks to all those who spent so much time working on the ancestors to this client, and to all contributors!

A big thank you to [Nudge Software] for backing this project.

[Apache Kafka protocol]:https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
[kafka-python]:https://github.com/mumrah/kafka-python
[David Arthur]:https://github.com/mumrah
[kafka-net]:https://github.com/Jroland/kafka-net
[jroland]:https://github.com/jroland
[KafkaNetClient]:https://github.com/gigya/KafkaNetClient
[gigya]:https://github.com/gigya
[Nudge Software]:http://nudge.ai
[AppVeyor]:https://www.appveyor.com/
