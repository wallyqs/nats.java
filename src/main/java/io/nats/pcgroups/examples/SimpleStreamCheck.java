package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;

public class SimpleStreamCheck {
    public static void main(String[] args) throws Exception {
        try (Connection connection = Nats.connect("nats://localhost:4222")) {
            JetStream js = connection.jetStream();
            
            System.out.println("=== Testing different subject patterns ===");
            
            // Try different subject patterns to see what works
            String[] testSubjects = {
                "events.>",        // Original subjects
                "*.events.>",      // Partitioned subjects  
                "*.>",             // All partitioned subjects
                ">",               // Everything
            };
            
            for (String subject : testSubjects) {
                System.out.println("\nTrying subject pattern: " + subject);
                try {
                    // Create ephemeral consumer for testing
                    JetStreamSubscription subscription = js.subscribe(subject);
                    
                    System.out.println("  Subscription created successfully");
                    
                    Message msg = subscription.nextMessage(Duration.ofSeconds(1));
                    if (msg != null) {
                        System.out.printf("  SUCCESS! Received message: [%s] %s%n", 
                            msg.getSubject(), new String(msg.getData()));
                        msg.ack();
                        
                        // Get a few more messages to see the pattern
                        for (int i = 0; i < 3; i++) {
                            Message nextMsg = subscription.nextMessage(Duration.ofMillis(500));
                            if (nextMsg != null) {
                                System.out.printf("  Next: [%s] %s%n", 
                                    nextMsg.getSubject(), new String(nextMsg.getData()));
                                nextMsg.ack();
                            }
                        }
                        
                        subscription.unsubscribe();
                        break; // Found the right pattern
                        
                    } else {
                        System.out.println("  No messages received");
                    }
                    
                    subscription.unsubscribe();
                    
                } catch (Exception e) {
                    System.out.printf("  Error: %s%n", e.getMessage());
                }
            }
        }
    }
}