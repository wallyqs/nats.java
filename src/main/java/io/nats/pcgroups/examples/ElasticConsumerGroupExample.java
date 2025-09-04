// Copyright 2024-2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.pcgroups.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example demonstrating elastic consumer groups functionality.
 * 
 * This example shows how to:
 * 1. Create a regular stream (no pre-partitioning required)
 * 2. Set up an elastic consumer group with work-queue stream
 * 3. Add/remove members dynamically
 * 4. Start consumer instances
 * 5. Publish messages that get automatically partitioned
 * 6. Observe dynamic membership changes
 */
public class ElasticConsumerGroupExample {
    private static final String STREAM_NAME = "demo-elastic-stream";
    private static final String CONSUMER_GROUP_NAME = "demo-elastic-cg";
    
    public static void main(String[] args) throws Exception {
        String natsUrl = "nats://localhost:4222";
        String command = args.length > 0 ? args[0] : "setup";
        String memberName = args.length > 1 ? args[1] : "member1";
        
        System.out.println("NATS Elastic Consumer Groups Demo");
        System.out.println("=================================");
        System.out.println("Command: " + command);
        
        try (Connection connection = Nats.connect(natsUrl)) {
            switch (command.toLowerCase()) {
                case "setup":
                    setupDemo(connection);
                    break;
                case "publish":
                    publishMessages(connection);
                    break;
                case "add":
                    if (args.length < 2) {
                        System.err.println("Usage: add <member1> [member2] ...");
                        return;
                    }
                    addMembers(connection, Arrays.copyOfRange(args, 1, args.length));
                    break;
                case "remove":
                    if (args.length < 2) {
                        System.err.println("Usage: remove <member1> [member2] ...");
                        return;
                    }
                    removeMembers(connection, Arrays.copyOfRange(args, 1, args.length));
                    break;
                case "consume":
                    startConsumer(connection, memberName);
                    break;
                case "status":
                    showStatus(connection);
                    break;
                case "cleanup":
                    cleanup(connection);
                    break;
                case "debug":
                    debugStreams(connection);
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    System.err.println("Available commands: setup, publish, add, remove, consume, status, cleanup, debug");
            }
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void setupDemo(Connection connection) throws Exception {
        System.out.println("\nSetting up elastic consumer group demo...");
        
        JetStreamManagement jsm = connection.jetStreamManagement();
        
        // Clean up existing resources first
        cleanup(connection);
        
        // Create original stream (no partitioning needed)
        System.out.println("Creating original stream: " + STREAM_NAME);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(STREAM_NAME)
            .subjects("events.>")
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(streamConfig);
        
        // Create elastic consumer group
        System.out.println("Creating elastic consumer group: " + CONSUMER_GROUP_NAME);
        ElasticConsumerGroupConfig config = ElasticConsumerGroups.createElastic(
            connection,
            STREAM_NAME,
            CONSUMER_GROUP_NAME,
            4,  // 4 partitions
            "events.>",  // Filter pattern
            Arrays.asList(1),  // Use first wildcard for partitioning
            1000,  // Max buffered messages
            50000  // Max buffered bytes
        );
        
        System.out.println("Elastic consumer group created with config:");
        System.out.println("  Max members: " + config.getMaxMembers());
        System.out.println("  Filter: " + config.getFilter());
        System.out.println("  Partitioning wildcards: " + config.getPartitioningWildcards());
        System.out.println("  Max buffered messages: " + config.getMaxBufferedMsgs());
        System.out.println("  Max buffered bytes: " + config.getMaxBufferedBytes());
        
        System.out.println("\nSetup complete!");
        System.out.println("\nNext steps:");
        System.out.println("1. Add members: java -cp ... ElasticConsumerGroupExample add member1 member2");
        System.out.println("2. Start consumers: java -cp ... ElasticConsumerGroupExample consume member1");
        System.out.println("3. Publish messages: java -cp ... ElasticConsumerGroupExample publish");
        System.out.println("4. Check status: java -cp ... ElasticConsumerGroupExample status");
    }
    
    private static void publishMessages(Connection connection) throws Exception {
        System.out.println("\nPublishing messages to elastic stream...");
        
        JetStream js = connection.jetStream();
        
        String[] eventTypes = {
            "events.user.created", 
            "events.user.updated", 
            "events.order.placed", 
            "events.order.shipped",
            "events.payment.processed",
            "events.inventory.updated"
        };
        
        for (int i = 0; i < 20; i++) {
            String subject = eventTypes[i % eventTypes.length];
            String message = String.format("Message %d for %s", i, subject);
            
            PublishAck ack = js.publish(subject, message.getBytes());
            System.out.printf("Published: %s (seq: %d)%n", message, ack.getSeqno());
            
            Thread.sleep(200); // Small delay between messages
        }
        
        System.out.println("\nFinished publishing messages");
    }
    
    private static void addMembers(Connection connection, String[] memberNames) throws Exception {
        System.out.println("\nAdding members to elastic consumer group...");
        
        List<String> added = ElasticConsumerGroups.addMembers(
            connection, STREAM_NAME, CONSUMER_GROUP_NAME, Arrays.asList(memberNames));
        
        if (added.isEmpty()) {
            System.out.println("No new members added (they may already exist or max members reached)");
        } else {
            System.out.println("Added members: " + added);
        }
        
        showStatus(connection);
    }
    
    private static void removeMembers(Connection connection, String[] memberNames) throws Exception {
        System.out.println("\nRemoving members from elastic consumer group...");
        
        List<String> removed = ElasticConsumerGroups.removeMembers(
            connection, STREAM_NAME, CONSUMER_GROUP_NAME, Arrays.asList(memberNames));
        
        if (removed.isEmpty()) {
            System.out.println("No members removed (they may not exist)");
        } else {
            System.out.println("Removed members: " + removed);
        }
        
        showStatus(connection);
    }
    
    private static void showStatus(Connection connection) throws Exception {
        System.out.println("\n=== Consumer Group Status ===");
        
        try {
            ElasticConsumerGroupConfig config = ElasticConsumerGroups.getElasticConsumerGroupConfig(
                connection, STREAM_NAME, CONSUMER_GROUP_NAME);
            
            System.out.println("Consumer Group: " + CONSUMER_GROUP_NAME);
            System.out.println("Original Stream: " + STREAM_NAME);
            System.out.println("Work-queue Stream: " + ElasticConsumerGroups.composeCGStreamName(STREAM_NAME, CONSUMER_GROUP_NAME));
            System.out.println("Max Members: " + config.getMaxMembers());
            System.out.println("Filter: " + config.getFilter());
            System.out.println("Current Members: " + (config.getMembers() != null ? config.getMembers() : "[]"));
            System.out.println("Member Count: " + (config.getMembers() != null ? config.getMembers().size() : 0));
            
            // Show partition assignments
            if (config.getMembers() != null && !config.getMembers().isEmpty()) {
                System.out.println("\nPartition Assignments:");
                for (String member : config.getMembers()) {
                    List<String> filters = ConsumerGroupUtils.generatePartitionFilters(
                        config.getMembers(), config.getMaxMembers(), 
                        config.getMemberMappings(), member);
                    System.out.println("  " + member + ": " + filters);
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error getting status: " + e.getMessage());
        }
    }
    
    private static void startConsumer(Connection connection, String memberName) throws Exception {
        System.out.println("\nStarting elastic consumer for member: " + memberName);
        
        AtomicInteger messageCount = new AtomicInteger(0);
        
        // Consumer configuration
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
            .ackPolicy(AckPolicy.Explicit)  // Required for elastic consumer groups
            .maxAckPending(1) // Ensure ordered processing
            .ackWait(Duration.ofSeconds(10))
            .build();
        
        // Start consuming
        ConsumerGroupConsumeContext context = ElasticConsumerGroups.elasticConsume(
            connection,
            STREAM_NAME,
            CONSUMER_GROUP_NAME,
            memberName,
            message -> {
                int count = messageCount.incrementAndGet();
                System.out.printf("[%s] Message %d: %s - %s%n", 
                    memberName, count, message.getSubject(), new String(message.getData()));
                
                // Acknowledge the message
                message.ack();
            },
            consumerConfig
        );
        
        System.out.println("Consumer started for member: " + memberName);
        System.out.println("Note: Consumer will only receive messages when member is in the group");
        System.out.println("Running for 30 seconds... Press ENTER to stop early");
        
        // Wait for user input or timeout after 30 seconds
        Scanner scanner = new Scanner(System.in);
        try {
            // Check if input is available for 30 seconds
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 30000) {
                if (scanner.hasNextLine()) {
                    scanner.nextLine();
                    break;
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.out.println("Input not available, running with timeout...");
            Thread.sleep(30000);
        }
        
        System.out.println("Stopping consumer...");
        context.stop();
        
        // Wait for completion
        try {
            context.done().get();
            System.out.println("Consumer stopped successfully");
        } catch (Exception e) {
            System.err.println("Error stopping consumer: " + e.getMessage());
        }
        
        System.out.printf("Processed %d messages%n", messageCount.get());
    }
    
    private static void cleanup(Connection connection) throws Exception {
        System.out.println("Cleaning up resources...");
        
        try {
            ElasticConsumerGroups.deleteElastic(connection, STREAM_NAME, CONSUMER_GROUP_NAME);
            System.out.println("Deleted elastic consumer group");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            jsm.deleteStream(STREAM_NAME);
            System.out.println("Deleted original stream");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        
        System.out.println("Cleanup complete");
    }
    
    private static void debugStreams(Connection connection) throws Exception {
        System.out.println("\n=== Debug Stream Information ===");
        
        JetStreamManagement jsm = connection.jetStreamManagement();
        
        System.out.println("\n--- Original Stream ---");
        try {
            StreamInfo originalStream = jsm.getStreamInfo(STREAM_NAME);
            System.out.println("Name: " + originalStream.getConfiguration().getName());
            System.out.println("Subjects: " + originalStream.getConfiguration().getSubjects());
            System.out.println("Messages: " + originalStream.getStreamState().getMsgCount());
            System.out.println("Bytes: " + originalStream.getStreamState().getByteCount());
        } catch (Exception e) {
            System.err.println("Error getting original stream: " + e.getMessage());
        }
        
        System.out.println("\n--- Work-queue Stream ---");
        String cgStreamName = STREAM_NAME + "-cg-" + CONSUMER_GROUP_NAME;
        try {
            StreamInfo cgStream = jsm.getStreamInfo(cgStreamName);
            System.out.println("Name: " + cgStream.getConfiguration().getName());
            System.out.println("Subjects: " + cgStream.getConfiguration().getSubjects());
            System.out.println("Sources: " + cgStream.getConfiguration().getSources());
            System.out.println("Messages: " + cgStream.getStreamState().getMsgCount());
            System.out.println("Bytes: " + cgStream.getStreamState().getByteCount());
            
            if (cgStream.getConfiguration().getSources() != null) {
                for (Source source : cgStream.getConfiguration().getSources()) {
                    System.out.println("Source: " + source.getName());
                    System.out.println("  Filter Subject: " + source.getFilterSubject());
                    System.out.println("  Subject Transforms: " + source.getSubjectTransforms());
                }
            }
        } catch (Exception e) {
            System.err.println("Error getting CG stream: " + e.getMessage());
        }
        
        System.out.println("\nDebug complete");
    }
}