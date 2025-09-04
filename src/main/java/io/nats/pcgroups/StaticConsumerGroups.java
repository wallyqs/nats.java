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

package io.nats.pcgroups;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.PriorityPolicy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Static consumer groups implementation
 */
public class StaticConsumerGroups {
    private static final String KV_STATIC_BUCKET_NAME = "static-consumer-groups";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Get the static consumer group's config from the KV bucket
     */
    public static StaticConsumerGroupConfig getStaticConsumerGroupConfig(Connection connection, 
                                                                        String streamName, 
                                                                        String consumerGroupName) 
            throws JetStreamApiException, IOException {
        KeyValue kv = connection.keyValue(KV_STATIC_BUCKET_NAME);
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);
        
        KeyValueEntry entry = kv.get(key);
        if (entry == null) {
            throw new IllegalArgumentException("Static consumer group config not found: " + key);
        }

        return objectMapper.readValue(entry.getValue(), StaticConsumerGroupConfig.class);
    }

    /**
     * Join and consume messages from a static consumer group
     */
    public static ConsumerGroupConsumeContext staticConsume(Connection connection,
                                                           String streamName,
                                                           String consumerGroupName,
                                                           String memberName,
                                                           Consumer<ConsumerGroupMsg> messageHandler,
                                                           ConsumerConfiguration config) 
            throws JetStreamApiException, IOException {

        if (messageHandler == null) {
            throw new IllegalArgumentException("A message handler must be provided");
        }

        JetStream jetStream = connection.jetStream();
        JetStreamManagement jsm = connection.jetStreamManagement();
        
        // Verify stream exists
        jsm.getStreamInfo(streamName);

        // Ensure minimum ack wait
        ConsumerConfiguration.Builder configBuilder = ConsumerConfiguration.builder(config);
        if (config.getAckWait() == null || config.getAckWait().compareTo(ConsumerGroupUtils.ACK_WAIT) < 0) {
            configBuilder.ackWait(ConsumerGroupUtils.ACK_WAIT);
        }

        return new StaticConsumerGroupInstance(connection, jetStream, streamName, consumerGroupName, 
                                             memberName, messageHandler, configBuilder.build());
    }

    /**
     * Internal implementation of static consumer group instance
     */
    private static class StaticConsumerGroupInstance implements ConsumerGroupConsumeContext {
        private final Connection connection;
        private final JetStream jetStream;
        private final String streamName;
        private final String consumerGroupName;
        private final String memberName;
        private final Consumer<ConsumerGroupMsg> messageHandler;
        private final ConsumerConfiguration userConfig;
        private final ExecutorService executor;
        private final CompletableFuture<Void> doneFuture;
        
        private volatile boolean stopped = false;
        private JetStreamSubscription subscription;

        public StaticConsumerGroupInstance(Connection connection,
                                         JetStream jetStream,
                                         String streamName,
                                         String consumerGroupName,
                                         String memberName,
                                         Consumer<ConsumerGroupMsg> messageHandler,
                                         ConsumerConfiguration userConfig) {
            this.connection = connection;
            this.jetStream = jetStream;
            this.streamName = streamName;
            this.consumerGroupName = consumerGroupName;
            this.memberName = memberName;
            this.messageHandler = messageHandler;
            this.userConfig = userConfig;
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "pcgroups-static-" + memberName);
                t.setDaemon(true);
                return t;
            });
            this.doneFuture = new CompletableFuture<>();
            
            // Start the consumption process
            executor.submit(this::consumeLoop);
        }

        @Override
        public void stop() {
            stopped = true;
            if (subscription != null) {
                try {
                    subscription.drain(Duration.ofSeconds(5));
                } catch (Exception e) {
                    // Ignore drain errors during shutdown
                }
            }
            executor.shutdown();
        }

        @Override
        public CompletableFuture<Void> done() {
            return doneFuture;
        }

        private void consumeLoop() {
            try {
                while (!stopped) {
                    try {
                        // Get current config
                        StaticConsumerGroupConfig config = getStaticConsumerGroupConfig(
                            connection, streamName, consumerGroupName);

                        // Check if this member is in the current membership
                        if (!config.isInMembership(memberName)) {
                            Thread.sleep(1000); // Wait before checking again
                            continue;
                        }

                        // Generate partition filters for this member
                        List<String> filters = ConsumerGroupUtils.generatePartitionFilters(
                            config.getMembers(), config.getMaxMembers(), 
                            config.getMemberMappings(), memberName);

                        if (filters.isEmpty()) {
                            Thread.sleep(1000);
                            continue;
                        }

                        // Create consumer configuration with priority group
                        String consumerName = consumerGroupName + "_" + memberName;
                        ConsumerConfiguration.Builder configBuilder = ConsumerConfiguration.builder(userConfig)
                            .durable(consumerName)
                            .filterSubjects(filters)
                            .priorityPolicy(PriorityPolicy.PinnedClient)
                            .pinnedTTL(Duration.ofSeconds(30))
                            .maxAckPending(1) // Ensure ordered processing
                            .ackWait(userConfig.getAckWait() != null ? userConfig.getAckWait() : ConsumerGroupUtils.ACK_WAIT);

                        // Create pull request options with priority group
                        PullRequestOptions pullRequestOptions = PullRequestOptions.builder(1)
                            .group(memberName)
                            .expiresIn(ConsumerGroupUtils.PULL_TIMEOUT)
                            .build();

                        // Create or update consumer using traditional pull subscribe
                        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                            .configuration(configBuilder.build())
                            .build();
                        
                        // Create subscription for pull-based consumption
                        subscription = jetStream.subscribe(null, pullSubscribeOptions);
                        
                        while (!stopped && config.isInMembership(memberName)) {
                            try {
                                Message msg = subscription.nextMessage(ConsumerGroupUtils.PULL_TIMEOUT);
                                if (msg != null && msg.isJetStream()) {
                                    ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(msg);
                                    messageHandler.accept(cgMsg);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                if (!stopped) {
                                    // Log error and continue
                                    System.err.println("Error processing message: " + e.getMessage());
                                }
                            }
                        }

                        // Clean up subscription
                        if (subscription != null) {
                            subscription.unsubscribe();
                            subscription = null;
                        }

                    } catch (Exception e) {
                        if (!stopped) {
                            System.err.println("Error in consume loop: " + e.getMessage());
                            Thread.sleep(1000); // Wait before retrying
                        }
                    }
                }
                
                doneFuture.complete(null);
                
            } catch (Exception e) {
                doneFuture.completeExceptionally(e);
            } finally {
                if (subscription != null) {
                    try {
                        subscription.unsubscribe();
                    } catch (Exception e) {
                        // Ignore cleanup errors
                    }
                }
            }
        }
    }
}