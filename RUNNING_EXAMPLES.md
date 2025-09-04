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

This library includes two types of consumer groups with separate example sets:

## 📊 Static Consumer Groups Examples

Static consumer groups require pre-partitioned streams and offer maximum performance.

### 1. 🛠️ Setup Static Demo Environment

```bash
gradle runStaticSetup
```

**What it does**:
- Creates `demo-partitioned-stream` with subject transform `events.> → {{partition(4,events.>)}}.events.>`
- Creates `static-consumer-groups` KV bucket
- Stores consumer group config with 2 members across 4 partitions

### 2. 📤 Publish Static Test Messages

```bash
gradle runStaticPublish
```

**What it does**:
- Publishes 20 messages across different subjects
- Messages are automatically partitioned by the stream's subject transform

### 3. 👥 Start Static Consumer Members

#### Member 1
```bash
gradle runStaticConsume1
```

#### Member 2
```bash
gradle runStaticConsume2
```

**Output**:
```
NATS Static Consumer Groups Demo
================================
Starting consumer for member: member1
Consumer started. Press ENTER to stop...
[member1] Message 1: events.user.created - Message 0 for events.user.created
...
```

## 🚀 Elastic Consumer Groups Examples

Elastic consumer groups work with any existing stream and support dynamic membership.

### 1. 🛠️ Setup Elastic Demo Environment

```bash
gradle runElasticSetup
```

**What it does**:
- Creates `demo-elastic-stream` (no pre-partitioning required)
- Creates work-queue stream `demo-elastic-stream-cg-demo-elastic-cg` 
- Sets up automatic subject transforms for partitioning
- Creates `elastic-consumer-groups` KV bucket

**Output**:
```
NATS Elastic Consumer Groups Demo
=================================
Command: setup

Setting up elastic consumer group demo...
Creating original stream: demo-elastic-stream
Creating elastic consumer group: demo-elastic-cg
Elastic consumer group created with config:
  Max members: 4
  Filter: events.>
  Partitioning wildcards: [1]
Setup complete!
```

### 2. 👥 Add Members to Elastic Group

```bash
gradle runElasticAdd
```

**What it does**:
- Adds `member1` and `member2` to the elastic consumer group
- Automatically assigns partitions and rebalances workload
- Updates configuration in KeyValue store

**Output**:
```
Adding members to elastic consumer group...
=== Consumer Group Status ===
Consumer Group: demo-elastic-cg
Max Members: 4
Current Members: [member1, member2]
Partition Assignments:
  member1: [0.>, 1.>]
  member2: [2.>, 3.>]
```

### 3. 📤 Publish Elastic Test Messages

```bash
gradle runElasticPublish
```

**What it does**:
- Publishes 20 messages to subjects like `events.user.created`, `events.order.placed`
- Messages are automatically transformed to partitioned subjects like `0.events.user.created`
- Work-queue stream receives the partitioned messages

### 4. 🎯 Start Elastic Consumers

#### Member 1
```bash
gradle runElasticConsume1
```

#### Member 2  
```bash
gradle runElasticConsume2
```

**What they do**:
- Join the elastic consumer group as the specified member
- Use pull-based batch processing for efficient message consumption
- Process only messages from their assigned partitions
- Automatically handle membership changes and rebalancing

**Output**:
```
NATS Elastic Consumer Groups Demo
=================================
Command: consume

Starting elastic consumer for member: member1
Consumer started for member: member1
Running for 30 seconds... Press ENTER to stop early
[member1] Message 1: events.user.created - Message 0 for events.user.created
[member1] Message 2: events.order.placed - Message 2 for events.order.placed
...
```

### 5. 📊 Check Elastic Consumer Group Status

```bash
gradle runElasticStatus
```

**What it does**:
- Shows current consumer group configuration
- Lists all members and their partition assignments  
- Displays stream information and message counts

**Output**:
```
=== Consumer Group Status ===
Consumer Group: demo-elastic-cg
Original Stream: demo-elastic-stream  
Work-queue Stream: demo-elastic-stream-cg-demo-elastic-cg
Max Members: 4
Filter: events.>
Current Members: [member1, member2]
Member Count: 2

Partition Assignments:
  member1: [0.>, 1.>] 
  member2: [2.>, 3.>]
```

### 6. 🧹 Cleanup Elastic Resources

```bash
gradle runElasticCleanup
```

**What it does**:
- Removes the elastic consumer group configuration
- Deletes the work-queue stream
- Deletes the original stream
- Cleans up KV bucket entries

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

## 🎯 Complete Demo Walkthroughs

### Static Consumer Groups Demo

Here's how to run a complete static consumer group demonstration:

#### Step 1: Setup
```bash
gradle runStaticSetup
```

#### Step 2: Publish Messages  
```bash
gradle runStaticPublish
```

#### Step 3: Start Consumers (in separate terminals)

**Terminal 1:**
```bash
gradle runStaticConsume1
# Wait for "Consumer started. Press ENTER to stop..."
```

**Terminal 2:**
```bash
gradle runStaticConsume2
# Wait for "Consumer started. Press ENTER to stop..."
```

### Elastic Consumer Groups Demo

Here's how to run a complete elastic consumer group demonstration:

#### Step 1: Setup
```bash
gradle runElasticSetup
```

#### Step 2: Add Members
```bash
gradle runElasticAdd
```

#### Step 3: Start Consumers (in separate terminals)

**Terminal 1:**
```bash
gradle runElasticConsume1
# Runs for 30 seconds or until ENTER pressed
```

**Terminal 2:**
```bash
gradle runElasticConsume2  
# Runs for 30 seconds or until ENTER pressed
```

#### Step 4: Publish Messages
```bash
# While consumers are running
gradle runElasticPublish
```

#### Step 5: Check Status
```bash
gradle runElasticStatus
```

#### Step 6: Test Dynamic Membership
```bash
# Add more members while consumers are running
gradle runElasticAdd member3 member4

# Check how partitions are rebalanced
gradle runElasticStatus

# Remove members
gradle runElasticRemove member3 member4
```

### Common Features Demonstrated

#### Partitioned Consumption
- Each member receives messages from their assigned partitions
- Member1 typically gets partitions 0,1; Member2 gets partitions 2,3
- Messages within each partition are processed in strict order

#### High Availability Testing
- Kill one consumer process (Ctrl+C) 
- Start multiple instances of the same member:
  ```bash
  # Start multiple member1 instances in different terminals
  gradle runElasticConsume1  # Terminal 1
  gradle runElasticConsume1  # Terminal 2  
  gradle runElasticConsume1  # Terminal 3
  ```
- Observe that only one instance receives messages (consumer pinning)
- Kill the active instance and watch failover to another instance

#### Pull-Based Batch Processing (Elastic Only)
- Consumers use `subscription.pull()` to request batches of up to 10 messages
- More efficient than single-message pulling
- Reduces network round trips and improves throughput

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

#### Static Consumer Groups
- ✅ **Pre-partitioned streams**: Maximum performance with subject transforms
- ✅ **Immutable configuration**: Stable partition assignments
- ✅ **Direct partitioning**: Messages routed to partitions at publish time

#### Elastic Consumer Groups  
- ✅ **Dynamic membership**: Add/remove members at runtime
- ✅ **Work-queue streams**: Automatic derivative stream creation
- ✅ **Pull-based batching**: Efficient message consumption with `pull()` API
- ✅ **Real-time rebalancing**: Partition reassignment on membership changes

#### Common Features (Both Types)
- ✅ **Partitioned consumption**: Messages split across members
- ✅ **Ordered processing**: Strict order within each partition  
- ✅ **High availability**: Multiple instances, only one active
- ✅ **Consumer pinning**: Automatic failover between instances
- ✅ **Subject transforms**: Automatic partitioning of published messages
- ✅ **Priority groups**: NATS server 2.11+ priority consumer features

## 🔧 Customization

### Change Member Names

#### Static Consumer Groups
```bash  
# Use custom member names (edit StaticConsumerGroupExample.java)
gradle runStaticConsume1 -Pargs="custom-member-1"
gradle runStaticConsume2 -Pargs="custom-member-2"
```

#### Elastic Consumer Groups  
```bash
# Add custom members
gradle runElasticAdd custom-member-1 custom-member-2

# Start consumers with custom names
gradle runElasticConsume1 -Pargs="consume,custom-member-1"
gradle runElasticConsume2 -Pargs="consume,custom-member-2"
```

### Use Different NATS Server
Edit the examples to change the default URL from `nats://localhost:4222` to your server URL.

### Modify Partition Count
- **Static**: Edit `StaticConsumerGroupExample.java` and change the `maxMembers` parameter
- **Elastic**: Edit `ElasticConsumerGroupExample.java` and modify the partition count in `createElastic()`

### Add More Members
- **Static**: Edit the consumer group configuration in `setupDemo()` to include additional members
- **Elastic**: Use `gradle runElasticAdd member3 member4 member5` to dynamically add members

### Performance Tuning

#### Batch Size (Elastic Only)
Edit `ElasticConsumerGroups.java` and modify the batch size in `PullRequestOptions.builder(10)` to adjust how many messages are fetched at once.

#### Buffer Limits (Elastic Only)  
Adjust `maxBufferedMessages` and `maxBufferedBytes` parameters in `createElastic()` to control work-queue stream resource usage.

#### ACK Wait Times
Modify `ackWait` in the `ConsumerConfiguration` to adjust how long to wait for message acknowledgments.