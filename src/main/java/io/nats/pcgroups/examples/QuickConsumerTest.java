// Quick test to verify messages were published and consumer groups work
package io.nats.pcgroups.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.pcgroups.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QuickConsumerTest {
    public static void main(String[] args) throws Exception {
        try (Connection connection = Nats.connect("nats://localhost:4222")) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            JetStream js = connection.jetStream();
            
            System.out.println("=== Stream Info ===");
            try {
                StreamInfo streamInfo = jsm.getStreamInfo("demo-partitioned-stream");
                System.out.println("Stream: " + streamInfo.getConfiguration().getName());
                System.out.println("Messages: " + streamInfo.getStreamState().getMsgCount());
                System.out.println("Subjects: " + streamInfo.getConfiguration().getSubjects());
                
                if (streamInfo.getConfiguration().getSubjectTransform() != null) {
                    System.out.println("Subject Transform: " + 
                        streamInfo.getConfiguration().getSubjectTransform().getSource() + " -> " +
                        streamInfo.getConfiguration().getSubjectTransform().getDestination());
                }
            } catch (Exception e) {
                System.out.println("Could not get stream info: " + e.getMessage());
                return;
            }
            
            System.out.println("\n=== Testing Consumer Group ===");
            
            CountDownLatch messageReceived = new CountDownLatch(5); // Wait for 5 messages
            AtomicInteger messageCount = new AtomicInteger(0);
            
            // Consumer configuration
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .maxAckPending(1) // Ensure ordered processing
                .ackWait(Duration.ofSeconds(10))
                .build();
            
            // Start consuming with timeout
            ConsumerGroupConsumeContext context = StaticConsumerGroups.staticConsume(
                connection,
                "demo-partitioned-stream",
                "demo-consumer-group",
                "member1",
                message -> {
                    int count = messageCount.incrementAndGet();
                    System.out.printf("[member1] Message %d: %s - %s%n", 
                        count, message.getSubject(), new String(message.getData()));
                    
                    // Acknowledge the message
                    message.ack();
                    messageReceived.countDown();
                },
                consumerConfig
            );
            
            System.out.println("Consumer started, waiting for messages...");
            
            // Wait for messages or timeout
            if (messageReceived.await(15, TimeUnit.SECONDS)) {
                System.out.println("Successfully received messages!");
            } else {
                System.out.println("Timeout waiting for messages");
            }
            
            System.out.println("Stopping consumer...");
            context.stop();
            
            try {
                context.done().get();
                System.out.println("Consumer stopped successfully");
            } catch (Exception e) {
                System.err.println("Error stopping consumer: " + e.getMessage());
            }
            
            System.out.printf("Total messages processed: %d%n", messageCount.get());
        }
    }
}