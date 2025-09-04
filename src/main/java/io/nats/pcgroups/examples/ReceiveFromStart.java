package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;

public class ReceiveFromStart {
    public static void main(String[] args) throws Exception {
        try (Connection connection = Nats.connect("nats://localhost:4222")) {
            JetStream js = connection.jetStream();
            JetStreamManagement jsm = connection.jetStreamManagement();
            
            System.out.println("=== Creating consumer to read from beginning ===");
            
            // Create a consumer that starts from the very beginning with partition filter
            ConsumerConfiguration config = ConsumerConfiguration.builder()
                .durable("partitioned-consumer")
                .deliverPolicy(DeliverPolicy.All) // Start from beginning
                .ackPolicy(AckPolicy.Explicit)
                .maxAckPending(1)
                .filterSubject("*.events.>") // Match partitioned subjects
                .build();
            
            try {
                jsm.createConsumer("demo-partitioned-stream", config);
                System.out.println("Consumer created");
                
                // Subscribe using the consumer
                JetStreamSubscription subscription = js.subscribe("*.events.>", 
                    PullSubscribeOptions.builder()
                        .durable("partitioned-consumer")
                        .build());
                
                System.out.println("Subscription created, pulling messages...");
                
                int messageCount = 0;
                for (int i = 0; i < 25; i++) { // Try to get more messages than we published
                    Message msg = subscription.nextMessage(Duration.ofSeconds(1));
                    if (msg != null) {
                        messageCount++;
                        System.out.printf("Message %d: [%s] %s%n", 
                            messageCount, msg.getSubject(), new String(msg.getData()));
                        msg.ack();
                    } else {
                        System.out.println("No more messages (timeout)");
                        break;
                    }
                }
                
                System.out.printf("Total messages received: %d%n", messageCount);
                subscription.unsubscribe();
                
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}