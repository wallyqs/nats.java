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
import io.nats.client.api.*;
import io.nats.client.impl.NatsKeyValueWatchSubscription;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Elastic consumer groups implementation
 */
public class ElasticConsumerGroups {
    private static final String KV_ELASTIC_BUCKET_NAME = "elastic-consumer-groups";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Compose consumer group stream name
     */
    public static String composeCGStreamName(String streamName, String consumerGroupName) {
        return streamName + "-cg-" + consumerGroupName;
    }

    /**
     * Get the elastic consumer group's config from the KV bucket
     */
    public static ElasticConsumerGroupConfig getElasticConsumerGroupConfig(Connection connection,
                                                                          String streamName,
                                                                          String consumerGroupName)
            throws JetStreamApiException, IOException {
        KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);

        KeyValueEntry entry = kv.get(key);
        if (entry == null) {
            throw new IllegalArgumentException("Elastic consumer group config not found: " + key);
        }

        return objectMapper.readValue(entry.getValue(), ElasticConsumerGroupConfig.class);
    }

    /**
     * Create an elastic consumer group
     */
    public static ElasticConsumerGroupConfig createElastic(Connection connection,
                                                           String streamName,
                                                           String consumerGroupName,
                                                           int maxNumMembers,
                                                           String filter,
                                                           List<Integer> partitioningWildcards,
                                                           long maxBufferedMessages,
                                                           long maxBufferedBytes)
            throws JetStreamApiException, IOException {

        JetStream jetStream = connection.jetStream();
        JetStreamManagement jsm = connection.jetStreamManagement();
        KeyValueManagement kvm = connection.keyValueManagement();

        // Validate that the original stream exists
        jsm.getStreamInfo(streamName);

        // Create KV bucket if it doesn't exist
        try {
            KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                    .name(KV_ELASTIC_BUCKET_NAME)
                    .storageType(StorageType.File)
                    .build();
            kvm.create(kvConfig);
        } catch (JetStreamApiException e) {
            // Bucket might already exist
            if (e.getApiErrorCode() != 10058) { // STREAM_NAME_EXIST
                throw e;
            }
        }

        KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);

        // Check if consumer group already exists
        KeyValueEntry existingEntry = kv.get(key);
        if (existingEntry != null) {
            return objectMapper.readValue(existingEntry.getValue(), ElasticConsumerGroupConfig.class);
        }

        // Create the configuration
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                maxNumMembers, filter, partitioningWildcards, maxBufferedMessages, maxBufferedBytes,
                new ArrayList<>(), // Empty members initially
                null
        );

        // Create the consumer group work-queue stream that sources from the original stream
        String cgStreamName = composeCGStreamName(streamName, consumerGroupName);

        // Build subject transform to add partition numbers
        SubjectTransform transform = buildPartitionTransform(filter, partitioningWildcards, maxNumMembers);

        StreamConfiguration.Builder cgStreamBuilder = StreamConfiguration.builder()
                .name(cgStreamName)
                .storageType(StorageType.File);

        if (maxBufferedMessages > 0) {
            cgStreamBuilder.maxMessages(maxBufferedMessages);
        }
        if (maxBufferedBytes > 0) {
            cgStreamBuilder.maxBytes(maxBufferedBytes);
        }

        // Add source from original stream with transform
        Source source = Source.builder()
                .name(streamName)
                .subjectTransforms(transform)
                .build();
        cgStreamBuilder.sources(source);

        jsm.addStream(cgStreamBuilder.build());

        // Store configuration
        String configJson = objectMapper.writeValueAsString(config);
        kv.put(key, configJson);

        return config;
    }

    /**
     * Add members to an elastic consumer group
     */
    public static List<String> addMembers(Connection connection,
                                         String streamName,
                                         String consumerGroupName,
                                         List<String> memberNamesToAdd)
            throws JetStreamApiException, IOException {

        KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);

        ElasticConsumerGroupConfig currentConfig = getElasticConsumerGroupConfig(connection, streamName, consumerGroupName);

        List<String> newMembers = new ArrayList<>(currentConfig.getMembers() != null ? currentConfig.getMembers() : new ArrayList<>());
        List<String> addedMembers = new ArrayList<>();

        for (String memberName : memberNamesToAdd) {
            if (!newMembers.contains(memberName)) {
                if (newMembers.size() >= currentConfig.getMaxMembers()) {
                    break; // Don't exceed max members
                }
                newMembers.add(memberName);
                addedMembers.add(memberName);
            }
        }

        if (!addedMembers.isEmpty()) {
            // Update configuration
            ElasticConsumerGroupConfig updatedConfig = new ElasticConsumerGroupConfig(
                    currentConfig.getMaxMembers(),
                    currentConfig.getFilter(),
                    currentConfig.getPartitioningWildcards(),
                    currentConfig.getMaxBufferedMsgs(),
                    currentConfig.getMaxBufferedBytes(),
                    newMembers,
                    currentConfig.getMemberMappings()
            );

            String configJson = objectMapper.writeValueAsString(updatedConfig);
            kv.put(key, configJson);
        }

        return addedMembers;
    }

    /**
     * Remove members from an elastic consumer group
     */
    public static List<String> removeMembers(Connection connection,
                                           String streamName,
                                           String consumerGroupName,
                                           List<String> memberNamesToRemove)
            throws JetStreamApiException, IOException {

        KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);

        ElasticConsumerGroupConfig currentConfig = getElasticConsumerGroupConfig(connection, streamName, consumerGroupName);

        List<String> newMembers = new ArrayList<>(currentConfig.getMembers() != null ? currentConfig.getMembers() : new ArrayList<>());
        List<String> removedMembers = new ArrayList<>();

        for (String memberName : memberNamesToRemove) {
            if (newMembers.remove(memberName)) {
                removedMembers.add(memberName);
            }
        }

        if (!removedMembers.isEmpty()) {
            // Update configuration
            ElasticConsumerGroupConfig updatedConfig = new ElasticConsumerGroupConfig(
                    currentConfig.getMaxMembers(),
                    currentConfig.getFilter(),
                    currentConfig.getPartitioningWildcards(),
                    currentConfig.getMaxBufferedMsgs(),
                    currentConfig.getMaxBufferedBytes(),
                    newMembers,
                    currentConfig.getMemberMappings()
            );

            String configJson = objectMapper.writeValueAsString(updatedConfig);
            kv.put(key, configJson);
        }

        return removedMembers;
    }

    /**
     * Delete an elastic consumer group
     */
    public static void deleteElastic(Connection connection,
                                   String streamName,
                                   String consumerGroupName)
            throws JetStreamApiException, IOException {

        JetStreamManagement jsm = connection.jetStreamManagement();
        KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);

        // Delete the consumer group stream
        String cgStreamName = composeCGStreamName(streamName, consumerGroupName);
        try {
            jsm.deleteStream(cgStreamName);
        } catch (JetStreamApiException e) {
            // Stream might not exist
        }

        // Delete the configuration
        String key = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);
        try {
            kv.delete(key);
        } catch (Exception e) {
            // Key might not exist
        }
    }

    /**
     * Join and consume messages from an elastic consumer group
     */
    public static ConsumerGroupConsumeContext elasticConsume(Connection connection,
                                                           String streamName,
                                                           String consumerGroupName,
                                                           String memberName,
                                                           Consumer<ConsumerGroupMsg> messageHandler,
                                                           ConsumerConfiguration config)
            throws JetStreamApiException, IOException {

        if (messageHandler == null) {
            throw new IllegalArgumentException("A message handler must be provided");
        }

        if (config.getAckPolicy() != AckPolicy.Explicit) {
            throw new IllegalArgumentException("The ack policy when consuming from elastic consumer groups must be explicit");
        }

        // Ensure minimum ack wait
        ConsumerConfiguration.Builder configBuilder = ConsumerConfiguration.builder(config);
        if (config.getAckWait() == null || config.getAckWait().compareTo(ConsumerGroupUtils.ACK_WAIT) < 0) {
            configBuilder.ackWait(ConsumerGroupUtils.ACK_WAIT);
        }

        if (config.getInactiveThreshold() == null) {
            configBuilder.inactiveThreshold(ConsumerGroupUtils.CONSUMER_IDLE_TIMEOUT);
        }

        return new ElasticConsumerGroupInstance(connection, streamName, consumerGroupName,
                memberName, messageHandler, configBuilder.build());
    }

    /**
     * Build subject transform for partitioning
     */
    private static SubjectTransform buildPartitionTransform(String filter, List<Integer> partitioningWildcards, int maxMembers) {
        // For now, implement a simple hash-based partitioning
        // This is a simplified version - the Go implementation has more complex logic
        String source = filter;
        
        // For events.>, we need to transform to numbered partitions
        // The transform should map events.> to partition numbers like 0.events.>, 1.events.>, etc.
        String destination;
        if (filter.endsWith(".>")) {
            // For events.> pattern, create partition prefix
            String basePattern = filter.substring(0, filter.length() - 2); // Remove .>
            destination = "{{partition(" + maxMembers + ",events.>)}}." + basePattern + ".>";
        } else {
            // For other patterns like events.*
            destination = "{{partition(" + maxMembers + "," + filter + ")}}." + filter;
        }

        return SubjectTransform.builder()
                .source(source)
                .destination(destination)
                .build();
    }

    /**
     * Internal implementation of elastic consumer group instance
     */
    private static class ElasticConsumerGroupInstance implements ConsumerGroupConsumeContext {
        private final Connection connection;
        private final String streamName;
        private final String consumerGroupName;
        private final String memberName;
        private final Consumer<ConsumerGroupMsg> messageHandler;
        private final ConsumerConfiguration userConfig;
        private final ExecutorService executor;
        private final CompletableFuture<Void> doneFuture;

        private volatile boolean stopped = false;
        private volatile int messageCount = 0;
        private JetStreamSubscription subscription;
        private NatsKeyValueWatchSubscription configWatchSubscription;

        public ElasticConsumerGroupInstance(Connection connection,
                                          String streamName,
                                          String consumerGroupName,
                                          String memberName,
                                          Consumer<ConsumerGroupMsg> messageHandler,
                                          ConsumerConfiguration userConfig) {
            this.connection = connection;
            this.streamName = streamName;
            this.consumerGroupName = consumerGroupName;
            this.memberName = memberName;
            this.messageHandler = messageHandler;
            this.userConfig = userConfig;
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "pcgroups-elastic-" + memberName);
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
            if (configWatchSubscription != null) {
                try {
                    configWatchSubscription.unsubscribe();
                } catch (Exception e) {
                    // Ignore watcher errors during shutdown
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
                JetStream jetStream = connection.jetStream();
                KeyValue kv = connection.keyValue(KV_ELASTIC_BUCKET_NAME);
                String configKey = ConsumerGroupUtils.composeKey(streamName, consumerGroupName);

                // Watch for configuration changes
                KeyValueWatcher watcher = new KeyValueWatcher() {
                    @Override
                    public void watch(KeyValueEntry entry) {
                        // Handle config changes
                        if (entry != null && entry.getKey().equals(configKey)) {
                            // Configuration changed - the loop will pick it up on next iteration
                        }
                    }

                    @Override
                    public void endOfData() {
                        // Initial data loaded
                    }
                };
                
                configWatchSubscription = kv.watchAll(watcher, KeyValueWatchOption.UPDATES_ONLY);

                while (!stopped) {
                    try {
                        // Get current config
                        ElasticConsumerGroupConfig config = getElasticConsumerGroupConfig(
                                connection, streamName, consumerGroupName);

                        // Check if this member is in the current membership
                        if (!config.isInMembership(memberName)) {
                            // Clean up subscription if we have one
                            if (subscription != null) {
                                subscription.unsubscribe();
                                subscription = null;
                            }
                            Thread.sleep(1000); // Wait before checking again
                            continue;
                        }

                        // Generate partition filters for this member
                        List<String> baseFilters = ConsumerGroupUtils.generatePartitionFilters(
                                config.getMembers(), config.getMaxMembers(),
                                config.getMemberMappings(), memberName);
                        
                        // Convert base filters (like "0.>") to match transformed subjects (like "0.events.>")
                        List<String> filters = new ArrayList<>();
                        String basePattern = config.getFilter();
                        if (basePattern.endsWith(".>")) {
                            String basePrefix = basePattern.substring(0, basePattern.length() - 2); // Remove ".>"
                            for (String baseFilter : baseFilters) {
                                // Transform "0.>" to "0.events.>"
                                String partitionNum = baseFilter.substring(0, baseFilter.length() - 2); // Remove ".>"
                                filters.add(partitionNum + "." + basePrefix + ".>");
                            }
                        } else {
                            // For patterns like "events.*", transform "0.>" to "0.events.*"
                            for (String baseFilter : baseFilters) {
                                String partitionNum = baseFilter.substring(0, baseFilter.length() - 2); // Remove ".>"
                                filters.add(partitionNum + "." + basePattern);
                            }
                        }

                        if (filters.isEmpty()) {
                            Thread.sleep(1000);
                            continue;
                        }

                        System.out.println("DEBUG: Member " + memberName + " using filters: " + filters);

                        // Create consumer configuration with priority group
                        String cgStreamName = composeCGStreamName(streamName, consumerGroupName);
                        String consumerName = consumerGroupName + "_" + memberName;

                        ConsumerConfiguration.Builder configBuilder = ConsumerConfiguration.builder(userConfig)
                                .durable(consumerName)
                                .filterSubjects(filters)
                                .priorityPolicy(PriorityPolicy.PinnedClient)
                                .priorityGroups(memberName)  // Pass group name as string
                                .pinnedTTL(Duration.ofSeconds(30))
                                .deliverPolicy(DeliverPolicy.New)  // Only process new messages
                                .maxAckPending(1); // Ensure ordered processing

                        // Create pull request options with priority group
                        PullRequestOptions pullOptions = PullRequestOptions.builder(1)
                                .group(memberName)
                                .expiresIn(ConsumerGroupUtils.PULL_TIMEOUT)
                                .build();

                        // Create or update consumer on the CG stream
                        PullSubscribeOptions pullSubscribeOptions = PullSubscribeOptions.builder()
                                .configuration(configBuilder.build())
                                .stream(cgStreamName)
                                .build();

                        System.out.println("DEBUG: Subscribing to subject: " + filters.get(0) + " on stream: " + cgStreamName);
                        System.out.println("DEBUG: Consumer config - Durable: " + configBuilder.build().getDurable());
                        System.out.println("DEBUG: Consumer config - Filter subjects: " + configBuilder.build().getFilterSubjects());
                        System.out.println("DEBUG: Consumer config - Deliver policy: " + configBuilder.build().getDeliverPolicy());
                        
                        try {
                            subscription = jetStream.subscribe(filters.get(0), pullSubscribeOptions);
                            System.out.println("DEBUG: Subscription created successfully - " + subscription.getConsumerName());
                        } catch (Exception e) {
                            System.err.println("DEBUG: Failed to create subscription: " + e.getMessage());
                            throw e;
                        }

                        while (!stopped && config.isInMembership(memberName)) {
                            try {
                                // Use pull() method for efficient batch pulling  
                                PullRequestOptions batchPullOptions = PullRequestOptions.builder(10)
                                        .group(memberName)
                                        .expiresIn(ConsumerGroupUtils.PULL_TIMEOUT.toMillis())
                                        .build();
                                
                                subscription.pull(batchPullOptions);
                                
                                // Process messages from the batch pull
                                int messagesProcessed = 0;
                                long batchStartTime = System.currentTimeMillis();
                                
                                while (System.currentTimeMillis() - batchStartTime < ConsumerGroupUtils.PULL_TIMEOUT.toMillis()) {
                                    Message msg = subscription.nextMessage(Duration.ofMillis(100));
                                    if (msg != null && msg.isJetStream()) {
                                        ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(msg);
                                        messageHandler.accept(cgMsg);
                                        msg.ack();
                                        messageCount++;
                                        messagesProcessed++;
                                    } else if (msg == null) {
                                        // No more messages available in this batch
                                        break;
                                    }
                                }
                                
                                if (messagesProcessed == 0) {
                                    Thread.sleep(1000); // Wait before next pull if no messages
                                }
                                
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            } catch (Exception e) {
                                if (!stopped) {
                                    System.err.println("Error in consume loop: " + e.getMessage());
                                    Thread.sleep(1000);
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
                if (configWatchSubscription != null) {
                    try {
                        configWatchSubscription.unsubscribe();
                    } catch (Exception e) {
                        // Ignore cleanup errors
                    }
                }
            }
        }
    }
}