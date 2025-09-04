# Running the NATS Partitioned Consumer Groups Examples

This guide shows how to run the various examples and demos included with the Java pcgroups library.

## Prerequisites

1. **NATS Server with JetStream**: You need a NATS server running with JetStream enabled
   ```bash
   # Option 1: Using nats-server directly
   nats-server -js
   
   # Option 2: Using Docker
   docker run -p 4222:4222 nats:latest -js
   
   # Option 3: Using the official NATS Docker image with custom config
   docker run -p 4222:4222 -p 8222:8222 nats:latest -js -m 8222
   ```

2. **Java 21+**: Make sure you have Java 21 or later installed
   ```bash
   java --version
   ```

3. **Build the Project**: 
   ```bash
   cd pcgroups.java
   gradle build
   ```

## Available Examples and Tasks

### 1. 🛠️ Setup Demo Environment

**Purpose**: Creates the stream, KV bucket, and consumer group configuration

```bash
gradle runSetup
```

**What it does**:
- Deletes any existing demo resources
- Creates `demo-partitioned-stream` with subject transform `events.> → {{partition(4,events.>)}}.events.>`
- Creates `static-consumer-groups` KV bucket
- Stores consumer group config with 2 members (`member1`, `member2`) across 4 partitions

**Output**:
```
NATS Partitioned Consumer Groups Demo
====================================
Connecting to NATS at: nats://localhost:4222

Setting up demo environment...
Creating partitioned stream: demo-partitioned-stream
Creating KV bucket for consumer group configs
Creating consumer group configuration

Setup complete!
```

### 2. 📤 Publish Test Messages

**Purpose**: Publishes sample messages that will be automatically partitioned

```bash
gradle runPublish
```

**What it does**:
- Publishes 20 messages across different subjects (`events.user.created`, `events.order.placed`, etc.)
- Messages are automatically partitioned by the stream's subject transform
- Each message gets assigned to partitions 0-3 based on subject hash

**Output**:
```
Publishing test messages...
Published: Message 0 for events.user.created (seq: 1)
Published: Message 1 for events.user.updated (seq: 2)
...
Finished publishing messages
```

### 3. 👥 Start Consumer Members

**Purpose**: Start consumer group members to process partitioned messages

#### Member 1
```bash
gradle runMember1
```

#### Member 2
```bash
gradle runMember2
```

**What they do**:
- Join the `demo-consumer-group` as the specified member
- Automatically get assigned their share of partitions
- Process messages with strict ordering within partitions
- Display received messages with partition info stripped

**Output**:
```
NATS Partitioned Consumer Groups Demo
====================================
Connecting to NATS at: nats://localhost:4222
Member name: member1

Starting consumer for member: member1
Consumer started. Press ENTER to stop...
[member1] Message 1: events.user.created - Message 0 for events.user.created
[member1] Message 2: events.order.placed - Message 2 for events.order.placed
...
```

### 4. 🔍 Debug and Inspection Tools

#### Quick Consumer Test
```bash
gradle runQuickTest
```
**Purpose**: Comprehensive test that shows stream info and runs a consumer for a few seconds

#### Stream Inspector
```bash
gradle runInspect
```
**Purpose**: Examines stream contents, existing consumers, and attempts direct message consumption

#### Simple Stream Check
```bash
gradle runSimpleCheck
```
**Purpose**: Tests different subject patterns to understand message routing

#### Debug Stream
```bash
gradle runDebug
```
**Purpose**: Tries various subscription filters to debug message delivery

#### Receive From Start
```bash
gradle runFromStart
```
**Purpose**: Creates a consumer that reads all messages from the beginning of the stream

## 🎯 Complete Demo Walkthrough

Here's how to run a complete demonstration:

### Step 1: Setup
```bash
# Clean setup
gradle runSetup
```

### Step 2: Publish Messages
```bash
# Publish test data
gradle runPublish
```

### Step 3: Start Consumers (in separate terminals)

**Terminal 1:**
```bash
gradle runMember1
# Wait for "Consumer started. Press ENTER to stop..."
```

**Terminal 2:**
```bash
gradle runMember2
# Wait for "Consumer started. Press ENTER to stop..."
```

### Step 4: Observe Partitioned Consumption
- Each member will receive messages from their assigned partitions
- Member1 typically gets partitions 0,1 (subjects that hash to these partitions)
- Member2 typically gets partitions 2,3 (subjects that hash to these partitions)
- Messages within each partition are processed in strict order

### Step 5: Test High Availability
- Kill one of the consumer processes (Ctrl+C)
- Start multiple instances of the same member:
  ```bash
  # Start multiple member1 instances in different terminals
  gradle runMember1  # Terminal 1
  gradle runMember1  # Terminal 2
  gradle runMember1  # Terminal 3
  ```
- Observe that only one instance receives messages (consumer pinning)
- Kill the active instance and watch failover to another instance

### Step 6: Cleanup
```bash
# Stop all consumers (press ENTER in each terminal)
# The setup task will clean up automatically on next run
```

## 🐛 Troubleshooting

### "Unable to connect to NATS servers"
- Ensure NATS server is running on localhost:4222
- Check server logs for any errors
- Try: `nats-server -js -V` (verbose mode)

### "No matching streams for subject"
- Run `gradle runSetup` to ensure stream is created
- Check that messages were published with `gradle runPublish`

### "Consumer already exists"
- Previous runs may have left consumers
- Run `gradle runSetup` to clean up and recreate

### No messages received
- Ensure messages were published after setup
- Previous consumers may have processed all messages
- Run the full sequence: setup → publish → consume

### Consumer stops immediately
- This is expected behavior - consumers wait for ENTER to stop
- Press ENTER to gracefully shut down a consumer

## 📊 Understanding the Output

### Message Format
```
[member1] Message 1: events.user.created - Message 0 for events.user.created
   ^         ^              ^                        ^
   |         |              |                        |
Member    Count    Original Subject          Message Content
```

### Partition Distribution
- **4 partitions total** (0, 1, 2, 3)
- **2 members** by default (member1, member2) 
- **Balanced distribution**: member1 gets partitions 0,1; member2 gets partitions 2,3
- Messages are assigned to partitions based on subject hash

### Consumer Group Features Demonstrated
- ✅ **Partitioned consumption**: Messages split across members
- ✅ **Ordered processing**: Strict order within each partition  
- ✅ **High availability**: Multiple instances, only one active
- ✅ **Consumer pinning**: Automatic failover between instances
- ✅ **Subject transform**: Automatic partitioning of published messages

## 🔧 Customization

### Change Member Names
```bash
# Use custom member names
gradle runMember1 -Pargs="nats://localhost:4222,custom-member-1"
gradle runMember2 -Pargs="nats://localhost:4222,custom-member-2"
```

### Use Different NATS Server
Edit the examples to change the default URL from `nats://localhost:4222` to your server URL.

### Modify Partition Count
Edit `StaticConsumerGroupExample.java` and change the `maxMembers` parameter in the configuration.

### Add More Members
Edit the consumer group configuration in `setupDemo()` to include additional members in the `members` list.