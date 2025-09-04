# NATS Partitioned Consumer Groups for Java

A Java implementation of partitioned consumer groups for NATS JetStream, leveraging priority groups and consumer pinning features introduced in NATS server 2.11.

This library is a port of the [orbit.go/pcgroups](https://github.com/synadia-io/orbit.go/tree/main/pcgroups) Go implementation, providing the same functionality for Java applications.

## Overview

This library enables horizontal scaling of message consumption from NATS JetStream while ensuring strict ordering within partitions. It solves the problem of achieving both high throughput and ordered processing by partitioning messages across multiple consumers.

### Key Features

- **Partitioned consumption**: Distribute messages across partitions while maintaining order within each partition
- **Priority groups**: Use NATS server's priority consumer groups with consumer pinning
- **High availability**: Multiple instances can run for the same member, with automatic failover
- **Ordered processing**: Strict ordering guarantees within partitions using `maxAckPending: 1`
- **Consumer group management**: Store configuration in NATS KV buckets

## Architecture

### Static Consumer Groups

- Require streams with pre-partitioned subjects (partition number as first token)
- Use subject transforms to automatically partition messages
- Configuration is immutable after creation
- Store config in `static-consumer-groups` KV bucket

Example partitioned subject: `0.events.user.created`, `1.events.user.updated`

### Priority Groups and Consumer Pinning

The library leverages NATS server 2.11+ features:
- **Priority Groups**: Multiple consumer instances compete for messages within a group
- **Consumer Pinning**: Only one instance per group receives messages at a time
- **Automatic Failover**: When the pinned consumer fails, another instance takes over

## Quick Start

### 1. Add Dependencies

```gradle
dependencies {
    implementation 'io.nats:jnats:2.22.0-SNAPSHOT'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
    implementation files('path/to/pcgroups-java-0.1.0-SNAPSHOT.jar')
}
```

### 2. Set Up Stream and Consumer Group

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

### 3. Start Consumer

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

### 4. Publish Messages

```java
JetStream js = connection.jetStream();

// Messages will be automatically partitioned by subject transform
js.publish("events.user.created", "user data".getBytes());
js.publish("events.order.placed", "order data".getBytes());
```

## API Reference

### Core Classes

- **`StaticConsumerGroups`**: Main entry point for static consumer group operations
- **`ConsumerGroupMsg`**: Message wrapper that strips partition numbers from subjects
- **`ConsumerGroupConsumeContext`**: Interface for controlling consumption
- **`StaticConsumerGroupConfig`**: Configuration for static consumer groups
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

See `src/main/java/io/nats/pcgroups/examples/StaticConsumerGroupExample.java` for a complete working example.

### Running the Example

1. Start NATS server with JetStream enabled:
   ```bash
   nats-server -js
   ```

2. Set up the demo environment:
   ```bash
   java -cp ... StaticConsumerGroupExample setup
   ```

3. Start consumers in separate terminals:
   ```bash
   java -cp ... StaticConsumerGroupExample nats://localhost:4222 member1
   java -cp ... StaticConsumerGroupExample nats://localhost:4222 member2
   ```

4. Publish test messages:
   ```bash
   java -cp ... StaticConsumerGroupExample publish
   ```

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