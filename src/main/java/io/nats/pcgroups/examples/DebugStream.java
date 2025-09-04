package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;

public class DebugStream {
    public static void main(String[] args) throws Exception {
        try (Connection connection = Nats.connect("nats://localhost:4222")) {
            JetStream js = connection.jetStream();
            
            System.out.println("=== Trying to consume directly from stream ===");
            
            // Try to consume directly with a simple consumer
            ConsumerConfiguration config = ConsumerConfiguration.builder()
                .durable("debug-consumer")
                .filterSubject("*.>") // Try to catch all partitioned subjects
                .maxAckPending(1)
                .build();
            
            try {
                JetStreamSubscription subscription = js.subscribe("", 
                    PullSubscribeOptions.builder().configuration(config).build());
                
                System.out.println("Waiting for messages...");
                for (int i = 0; i < 5; i++) {
                    Message msg = subscription.nextMessage(Duration.ofSeconds(2));
                    if (msg != null) {
                        System.out.printf("Received: [%s] %s%n", msg.getSubject(), new String(msg.getData()));
                        msg.ack();
                    } else {
                        System.out.println("No message received in timeout");
                        break;
                    }
                }
                
                subscription.unsubscribe();
                
            } catch (Exception e) {
                System.err.println("Error creating subscription: " + e.getMessage());
            }
            
            // Also try with specific partition filters
            System.out.println("\n=== Trying specific partition filters ===");
            String[] filters = {"0.>", "1.>", "2.>", "3.>"};
            
            for (String filter : filters) {
                System.out.println("Trying filter: " + filter);
                try {
                    ConsumerConfiguration partitionConfig = ConsumerConfiguration.builder()
                        .durable("debug-partition-" + filter.charAt(0))
                        .filterSubject(filter)
                        .maxAckPending(1)
                        .build();
                    
                    JetStreamSubscription subscription = js.subscribe("", 
                        PullSubscribeOptions.builder().configuration(partitionConfig).build());
                    
                    Message msg = subscription.nextMessage(Duration.ofSeconds(1));
                    if (msg != null) {
                        System.out.printf("  Found message: [%s] %s%n", msg.getSubject(), new String(msg.getData()));
                        msg.ack();
                    } else {
                        System.out.println("  No messages for filter: " + filter);
                    }
                    
                    subscription.unsubscribe();
                    
                } catch (Exception e) {
                    System.err.printf("  Error with filter %s: %s%n", filter, e.getMessage());
                }
            }
        }
    }
}