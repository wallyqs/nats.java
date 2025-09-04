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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.pcgroups.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example demonstrating static consumer groups functionality.
 * 
 * This example shows how to:
 * 1. Create a stream with partitioned subjects
 * 2. Set up a static consumer group configuration
 * 3. Start multiple consumer instances
 * 4. Publish partitioned messages
 * 5. Observe ordered consumption within partitions
 */
public class StaticConsumerGroupExample {
    private static final String STREAM_NAME = "demo-partitioned-stream";
    private static final String CONSUMER_GROUP_NAME = "demo-consumer-group";
    
    public static void main(String[] args) throws Exception {
        String natsUrl = "nats://localhost:4222";
        String memberName = "member1";
        
        if (args.length == 0) {
            // Default to setup
            System.out.println("NATS Partitioned Consumer Groups Demo");
            System.out.println("====================================");
            System.out.println("Connecting to NATS at: " + natsUrl);
            System.out.println("Member name: " + memberName);
            
            try (Connection connection = Nats.connect(natsUrl)) {
                setupDemo(connection);
            }
            return;
        }
        
        if ("publish".equals(args[0])) {
            System.out.println("NATS Partitioned Consumer Groups Demo");
            System.out.println("====================================");
            System.out.println("Publishing messages to: " + natsUrl);
            
            try (Connection connection = Nats.connect(natsUrl)) {
                publishMessages(connection);
            }
            return;
        }
        
        // Consumer mode: args[0] = natsUrl, args[1] = memberName
        if (args.length >= 2) {
            natsUrl = args[0];
            memberName = args[1];
        }
        
        System.out.println("NATS Partitioned Consumer Groups Demo");
        System.out.println("====================================");
        System.out.println("Connecting to NATS at: " + natsUrl);
        System.out.println("Member name: " + memberName);
        
        try (Connection connection = Nats.connect(natsUrl)) {
            // Start consumer
            startConsumer(connection, memberName);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void setupDemo(Connection connection) throws Exception {
        System.out.println("\nSetting up demo environment...");
        
        JetStreamManagement jsm = connection.jetStreamManagement();
        KeyValueManagement kvm = connection.keyValueManagement();
        
        // Clean up existing resources
        try {
            jsm.deleteStream(STREAM_NAME);
        } catch (Exception e) {
            // Ignore if stream doesn't exist
        }
        
        try {
            kvm.delete("static-consumer-groups");
        } catch (Exception e) {
            // Ignore if bucket doesn't exist
        }
        
        // Create partitioned stream with subject transform
        System.out.println("Creating partitioned stream: " + STREAM_NAME);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .name(STREAM_NAME)
            .subjects("events.>")
            // Subject transform to add partition number as first token
            .subjectTransform(SubjectTransform.builder()
                .source("events.>")
                .destination("{{partition(4,events.>)}}.events.>")
                .build())
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(streamConfig);
        
        // Create KV bucket for consumer group configs
        System.out.println("Creating KV bucket for consumer group configs");
        KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
            .name("static-consumer-groups")
            .storageType(StorageType.Memory)
            .build();
        kvm.create(kvConfig);
        
        // Create static consumer group configuration
        System.out.println("Creating consumer group configuration");
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            4, // 4 partitions
            "*.>", // Filter all partitions
            Arrays.asList("member1", "member2"), // Two members
            null // Use balanced distribution
        );
        
        KeyValue kv = connection.keyValue("static-consumer-groups");
        ObjectMapper mapper = new ObjectMapper();
        String configJson = mapper.writeValueAsString(config);
        kv.put(ConsumerGroupUtils.composeKey(STREAM_NAME, CONSUMER_GROUP_NAME), configJson);
        
        System.out.println("\nSetup complete!");
        System.out.println("\nTo start consumers, run:");
        System.out.println("  java -cp ... StaticConsumerGroupExample nats://localhost:4222 member1");
        System.out.println("  java -cp ... StaticConsumerGroupExample nats://localhost:4222 member2");
        System.out.println("\nTo publish test messages, run:");
        System.out.println("  java -cp ... StaticConsumerGroupExample publish");
    }
    
    private static void publishMessages(Connection connection) throws Exception {
        System.out.println("\nPublishing test messages...");
        
        JetStream js = connection.jetStream();
        
        // Publish messages to different subjects (will be partitioned automatically)
        String[] subjects = {
            "events.user.created", 
            "events.user.updated", 
            "events.order.placed", 
            "events.order.shipped",
            "events.payment.processed"
        };
        
        for (int i = 0; i < 20; i++) {
            String subject = subjects[i % subjects.length];
            String message = String.format("Message %d for %s", i, subject);
            
            PublishAck ack = js.publish(subject, message.getBytes());
            System.out.printf("Published: %s (seq: %d)%n", message, ack.getSeqno());
            
            Thread.sleep(500); // Small delay between messages
        }
        
        System.out.println("\nFinished publishing messages");
    }
    
    private static void startConsumer(Connection connection, String memberName) throws Exception {
        System.out.println("\nStarting consumer for member: " + memberName);
        
        AtomicInteger messageCount = new AtomicInteger(0);
        
        // Consumer configuration
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
            .maxAckPending(1) // Ensure ordered processing
            .ackWait(Duration.ofSeconds(10))
            .build();
        
        // Start consuming
        ConsumerGroupConsumeContext context = StaticConsumerGroups.staticConsume(
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
        
        System.out.println("Consumer started. Press ENTER to stop...");
        new Scanner(System.in).nextLine();
        
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
}