# NATS Partitioned Consumer Groups for Java

A Java implementation of partitioned consumer groups for NATS JetStream, leveraging priority groups and consumer pinning features introduced in NATS server 2.11.

This library is a port of the [orbit.go/pcgroups](https://github.com/synadia-io/orbit.go/tree/main/pcgroups) Go implementation, providing the same functionality for Java applications.

## Overview

This library enables horizontal scaling of message consumption from NATS JetStream while ensuring strict ordering within partitions. It solves the problem of achieving both high throughput and ordered processing by partitioning messages across multiple consumers.

### Key Features

- **Partitioned consumption**: Distribute messages across partitions while maintaining order within each partition
- **Two deployment models**: Static (pre-partitioned streams) and Elastic (dynamic partitioning)
- **Priority groups**: Use NATS server's priority consumer groups with consumer pinning
- **High availability**: Multiple instances can run for the same member, with automatic failover
- **Ordered processing**: Strict ordering guarantees within partitions using `maxAckPending: 1`
- **Dynamic membership**: Add/remove members at runtime with automatic partition rebalancing
- **Consumer group management**: Store configuration in NATS KV buckets

## Architecture

### Static Consumer Groups

- Require streams with pre-partitioned subjects (partition number as first token)
- Use subject transforms to automatically partition messages
- Configuration is immutable after creation
- Store config in `static-consumer-groups` KV bucket

Example partitioned subject: `0.events.user.created`, `1.events.user.updated`

### Elastic Consumer Groups

- Work with any existing stream - no pre-partitioning required
- Create derivative work-queue streams that source from original stream
- Support dynamic membership changes at runtime
- Use subject transforms for automatic message partitioning
- Store config in `elastic-consumer-groups` KV bucket

Example: Original subject `events.user.created` → Partitioned subject `0.events.user.created`

### Priority Groups and Consumer Pinning

The library leverages NATS server 2.11+ features:
- **Priority Groups**: Multiple consumer instances compete for messages within a group
- **Consumer Pinning**: Only one instance per group receives messages at a time
- **Automatic Failover**: When the pinned consumer fails, another instance takes over

## Quick Start

Choose between Static or Elastic consumer groups based on your requirements:

- **Static**: Use when you have pre-partitioned streams and want maximum performance
- **Elastic**: Use when you want dynamic membership and work with existing streams

### 1. Add Dependencies

```gradle
dependencies {
    implementation 'io.nats:jnats:2.22.0-SNAPSHOT'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
    implementation files('path/to/pcgroups-java-0.1.0-SNAPSHOT.jar')
}
```

### 2A. Static Consumer Groups - Setup

```java
import io.nats.pcgroups.*;
import io.nats.client.*;
import io.nats.client.api.*;

// Create partitioned stream with subject transform
StreamConfiguration streamConfig = StreamConfiguration.builder()
    .name("events-stream")
    .subjects("events.>")
    .subjectTransform(SubjectTransform.builder()
        .source("events.>")
        .destination("{{partition(4,events.>)}}.events.>")
        .build())
    .build();
jsm.addStream(streamConfig);

// Create consumer group configuration
StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
    4, // 4 partitions
    "*.>", // Filter all partitions  
    Arrays.asList("member1", "member2"), // Two members
    null // Use balanced distribution
);

// Store configuration in KV bucket
KeyValue kv = connection.keyValue("static-consumer-groups");
ObjectMapper mapper = new ObjectMapper();
String configJson = mapper.writeValueAsString(config);
kv.put("events-stream.my-consumer-group", configJson);
```

### 2B. Elastic Consumer Groups - Setup

```java
// Create original stream (no partitioning needed)
StreamConfiguration streamConfig = StreamConfiguration.builder()
    .name("events-stream")
    .subjects("events.>")
    .build();
jsm.addStream(streamConfig);

// Create elastic consumer group with dynamic partitioning
ElasticConsumerGroupConfig config = ElasticConsumerGroups.createElastic(
    connection,
    "events-stream",
    "my-elastic-group",
    4,  // 4 partitions
    "events.>",  // Filter pattern
    Arrays.asList(1),  // Use first wildcard for partitioning
    1000,  // Max buffered messages
    50000  // Max buffered bytes
);

// Add members to the group
ElasticConsumerGroups.addMembers(connection, "events-stream", "my-elastic-group", "member1", "member2");
```

### 3A. Static Consumer - Start Consuming

```java
// Consumer configuration
ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
    .maxAckPending(1) // Ensure ordered processing
    .ackWait(Duration.ofSeconds(10))
    .build();

// Start consuming
ConsumerGroupConsumeContext context = StaticConsumerGroups.staticConsume(
    connection,
    "events-stream",
    "my-consumer-group", 
    "member1", // Member name
    message -> {
        // Process message (subject has partition number stripped)
        System.out.println("Received: " + message.getSubject() + " - " + new String(message.getData()));
        message.ack();
    },
    consumerConfig
);

// Stop when done
context.stop();
context.done().get();
```

### 3B. Elastic Consumer - Start Consuming

```java
// Consumer configuration
ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
    .ackPolicy(AckPolicy.Explicit)  // Required for elastic consumer groups
    .maxAckPending(1) // Ensure ordered processing
    .ackWait(Duration.ofSeconds(10))
    .build();

// Start consuming
ConsumerGroupConsumeContext context = ElasticConsumerGroups.elasticConsume(
    connection,
    "events-stream",
    "my-elastic-group",
    "member1", // Member name
    message -> {
        // Process message (subject has partition number stripped)
        System.out.println("Received: " + message.getSubject() + " - " + new String(message.getData()));
    },
    consumerConfig
);

// Stop when done
context.stop();
context.done().get();
```

### 4. Publish Messages

```java
JetStream js = connection.jetStream();

// Messages will be automatically partitioned
js.publish("events.user.created", "user data".getBytes());
js.publish("events.order.placed", "order data".getBytes());
```

## API Reference

### Core Classes

#### Static Consumer Groups
- **`StaticConsumerGroups`**: Main entry point for static consumer group operations
- **`StaticConsumerGroupConfig`**: Configuration for static consumer groups

#### Elastic Consumer Groups
- **`ElasticConsumerGroups`**: Main entry point for elastic consumer group operations  
- **`ElasticConsumerGroupConfig`**: Configuration for elastic consumer groups

#### Shared Classes
- **`ConsumerGroupMsg`**: Message wrapper that strips partition numbers from subjects
- **`ConsumerGroupConsumeContext`**: Interface for controlling consumption
- **`ConsumerGroupUtils`**: Utility functions including partition filter generation

### Key Methods

#### StaticConsumerGroups.staticConsume()

```java
public static ConsumerGroupConsumeContext staticConsume(
    Connection connection,
    String streamName,
    String consumerGroupName,
    String memberName,
    Consumer<ConsumerGroupMsg> messageHandler,
    ConsumerConfiguration config
)
```

Joins a static consumer group and starts consuming messages.

#### ElasticConsumerGroups.createElastic()

```java
public static ElasticConsumerGroupConfig createElastic(
    Connection connection,
    String streamName,
    String consumerGroupName,
    int maxNumMembers,
    String filter,
    List<Integer> partitioningWildcards,
    long maxBufferedMessages,
    long maxBufferedBytes
) throws JetStreamApiException, IOException
```

Creates a new elastic consumer group with the specified configuration.

#### ElasticConsumerGroups.elasticConsume()

```java
public static ConsumerGroupConsumeContext elasticConsume(
    Connection connection,
    String streamName,
    String consumerGroupName,
    String memberName,
    Consumer<ConsumerGroupMsg> messageHandler,
    ConsumerConfiguration config
) throws JetStreamApiException, IOException
```

Joins an elastic consumer group and starts consuming messages with dynamic membership.

#### ElasticConsumerGroups.addMembers()

```java
public static void addMembers(
    Connection connection,
    String streamName,
    String consumerGroupName,
    String... memberNames
) throws JetStreamApiException, IOException
```

Adds members to an elastic consumer group with automatic partition rebalancing.

#### ConsumerGroupUtils.generatePartitionFilters()

```java
public static List<String> generatePartitionFilters(
    List<String> members,
    int maxMembers,
    List<MemberMapping> memberMappings,
    String memberName
)
```

Generates partition filters for a specific member based on balanced distribution or custom mappings.

## Configuration

### Balanced Distribution

Members are automatically assigned partitions evenly:

```java
StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
    8, // 8 partitions
    "*.>",
    Arrays.asList("member1", "member2", "member3"), // 3 members
    null // Balanced: member1 gets 0,1,2 | member2 gets 3,4,5 | member3 gets 6,7
);
```

### Custom Mappings

Explicitly assign partitions to members:

```java
List<MemberMapping> mappings = Arrays.asList(
    new MemberMapping("member1", Arrays.asList(0, 2, 4, 6)),
    new MemberMapping("member2", Arrays.asList(1, 3, 5, 7))
);

StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
    8, "*.>", null, mappings
);
```

## Examples

See the complete working examples in `src/main/java/io/nats/pcgroups/examples/`:
- `StaticConsumerGroupExample.java` - Static consumer groups demo
- `ElasticConsumerGroupExample.java` - Elastic consumer groups demo

### Running Examples with Gradle

1. Start NATS server with JetStream enabled:
   ```bash
   nats-server -js
   ```

#### Static Consumer Group Example

2. Set up the static demo environment:
   ```bash
   gradle runStaticSetup
   ```

3. Start consumers in separate terminals:
   ```bash
   gradle runStaticConsume1  # Terminal 1
   gradle runStaticConsume2  # Terminal 2
   ```

4. Publish test messages:
   ```bash
   gradle runStaticPublish
   ```

#### Elastic Consumer Group Example

2. Set up the elastic demo environment:
   ```bash
   gradle runElasticSetup
   ```

3. Add members to the consumer group:
   ```bash
   gradle runElasticAdd
   ```

4. Start consumers in separate terminals:
   ```bash
   gradle runElasticConsume1  # Terminal 1
   gradle runElasticConsume2  # Terminal 2
   ```

5. Publish test messages:
   ```bash
   gradle runElasticPublish
   ```

6. Check consumer group status:
   ```bash
   gradle runElasticStatus
   ```

See [RUNNING_EXAMPLES.md](RUNNING_EXAMPLES.md) for detailed instructions and available commands.

## Testing

Run the test suite:

```bash
./gradlew test
```

For integration tests that require a running NATS server:

```bash
NATS_URL=nats://localhost:4222 ./gradlew test
```

## Requirements

- Java 21+
- NATS Server 2.11+ with JetStream enabled
- NATS Java Client 2.22.0+

## Differences from Go Implementation

This Java implementation maintains the same core functionality as the Go version while adapting to Java idioms:

- Uses Java's `Consumer<T>` interface for message handlers instead of function callbacks
- Uses `CompletableFuture<Void>` instead of Go channels for completion signaling
- Jackson JSON library for configuration serialization instead of Go's built-in JSON
- Java executors for background processing instead of goroutines

## Contributing

This library is part of the NATS ecosystem. Please follow the same contribution guidelines as other NATS projects.

## License

Apache License 2.0 - see LICENSE file for details.