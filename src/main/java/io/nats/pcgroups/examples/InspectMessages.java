package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;

public class InspectMessages {
    public static void main(String[] args) throws Exception {
        try (Connection connection = Nats.connect("nats://localhost:4222")) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            JetStream js = connection.jetStream();
            
            System.out.println("=== Stream and Consumer Information ===");
            
            try {
                StreamInfo streamInfo = jsm.getStreamInfo("demo-partitioned-stream");
                System.out.println("Stream: " + streamInfo.getConfiguration().getName());
                System.out.println("Messages: " + streamInfo.getStreamState().getMsgCount());
                System.out.println("Subjects in stream config: " + streamInfo.getConfiguration().getSubjects());
                
                // Note: Subject details may not be available in this NATS version
                
                // List consumers
                System.out.println("\n=== Existing Consumers ===");
                try {
                    for (ConsumerInfo consumerInfo : jsm.getConsumers("demo-partitioned-stream")) {
                        System.out.println("Consumer: " + consumerInfo.getName());
                        System.out.println("  Delivered: " + consumerInfo.getDelivered().getConsumerSequence());
                        System.out.println("  Num Ack Pending: " + consumerInfo.getNumAckPending());
                        System.out.println("  Filter Subject: " + consumerInfo.getConsumerConfiguration().getFilterSubject());
                        if (consumerInfo.getConsumerConfiguration().getFilterSubjects() != null) {
                            System.out.println("  Filter Subjects: " + consumerInfo.getConsumerConfiguration().getFilterSubjects());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("No consumers or error listing: " + e.getMessage());
                }
                
                // Try to create a consumer without any filter to see all messages
                System.out.println("\n=== Creating consumer without filter ===");
                
                ConsumerConfiguration noFilterConfig = ConsumerConfiguration.builder()
                    .durable("inspect-all")
                    .maxAckPending(10)
                    .ackWait(Duration.ofSeconds(30))
                    .build();
                
                try {
                    jsm.createConsumer("demo-partitioned-stream", noFilterConfig);
                    System.out.println("Consumer created successfully");
                    
                    JetStreamSubscription subscription = js.subscribe("events.>", 
                        PullSubscribeOptions.builder().configuration(noFilterConfig).build());
                    
                    System.out.println("Pulling messages...");
                    for (int i = 0; i < 10; i++) {
                        Message msg = subscription.nextMessage(Duration.ofSeconds(2));
                        if (msg != null) {
                            System.out.printf("Message %d: [%s] %s%n", i+1, msg.getSubject(), new String(msg.getData()));
                            msg.ack();
                        } else {
                            System.out.println("No more messages");
                            break;
                        }
                    }
                    
                    subscription.unsubscribe();
                    
                } catch (Exception e) {
                    System.err.println("Error creating/using unfiltered consumer: " + e.getMessage());
                    e.printStackTrace();
                }
                
            } catch (Exception e) {
                System.err.println("Error inspecting stream: " + e.getMessage());
            }
        }
    }
}