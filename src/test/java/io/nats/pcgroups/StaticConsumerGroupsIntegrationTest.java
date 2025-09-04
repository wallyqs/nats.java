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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for StaticConsumerGroups.
 * These tests require a running NATS server with JetStream enabled.
 * Set environment variable NATS_URL to run these tests (e.g., NATS_URL=nats://localhost:4222)
 */
@EnabledIfEnvironmentVariable(named = "NATS_URL", matches = ".*")
class StaticConsumerGroupsIntegrationTest {

    private static final String TEST_STREAM = "test-pcgroups-stream";
    private static final String TEST_CONSUMER_GROUP = "test-cg";
    
    @Test
    void testStaticConsumerGroupCreationAndConfiguration() throws Exception {
        String natsUrl = System.getenv("NATS_URL");
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            KeyValueManagement kvm = connection.keyValueManagement();

            // Clean up first
            cleanup(jsm, kvm);

            // Create test stream
            createTestStream(jsm);

            // Create KV bucket for static consumer groups
            createKvBucket(kvm);

            // Create static consumer group config
            List<String> members = Arrays.asList("member1", "member2");
            StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "*.>", members, null);

            // Store config in KV
            KeyValue kv = connection.keyValue("static-consumer-groups");
            ObjectMapper mapper = new ObjectMapper();
            String configJson = mapper.writeValueAsString(config);
            kv.put(ConsumerGroupUtils.composeKey(TEST_STREAM, TEST_CONSUMER_GROUP), configJson);

            // Test config retrieval
            StaticConsumerGroupConfig retrievedConfig = StaticConsumerGroups.getStaticConsumerGroupConfig(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP);

            assertEquals(config.getMaxMembers(), retrievedConfig.getMaxMembers());
            assertEquals(config.getFilter(), retrievedConfig.getFilter());
            assertEquals(config.getMembers(), retrievedConfig.getMembers());
            assertTrue(retrievedConfig.isInMembership("member1"));
            assertTrue(retrievedConfig.isInMembership("member2"));
            assertFalse(retrievedConfig.isInMembership("member3"));
        }
    }

    @Test
    void testPartitionFilterGeneration() {
        List<String> members = Arrays.asList("member1", "member2");
        int maxMembers = 4;

        // Test balanced distribution
        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member1");
        assertEquals(Arrays.asList("0.>", "1.>"), filters1);

        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member2");
        assertEquals(Arrays.asList("2.>", "3.>"), filters2);

        // Test with member mappings
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member1", Arrays.asList(0, 3)),
            new MemberMapping("member2", Arrays.asList(1, 2))
        );

        List<String> mappingFilters1 = ConsumerGroupUtils.generatePartitionFilters(
            null, maxMembers, mappings, "member1");
        assertEquals(Arrays.asList("0.>", "3.>"), mappingFilters1);

        List<String> mappingFilters2 = ConsumerGroupUtils.generatePartitionFilters(
            null, maxMembers, mappings, "member2");
        assertEquals(Arrays.asList("1.>", "2.>"), mappingFilters2);
    }

    @Test
    void testConsumerGroupMessageWrapper() throws Exception {
        String natsUrl = System.getenv("NATS_URL");
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            JetStream js = connection.jetStream();

            // Clean up and create test stream
            cleanup(jsm, connection.keyValueManagement());
            createTestStream(jsm);

            // Publish a test message with partition prefix
            js.publish("0.events.user.created", "test message".getBytes());

            // Create a simple consumer to receive the message
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable("test-consumer")
                .filterSubject("0.events.>")
                .build();

            jsm.createConsumer(TEST_STREAM, consumerConfig);
            JetStreamSubscription subscription = js.subscribe("", 
                PullSubscribeOptions.builder().configuration(consumerConfig).build());

            Message msg = subscription.nextMessage(Duration.ofSeconds(5));
            assertNotNull(msg);
            assertEquals("0.events.user.created", msg.getSubject());

            // Test ConsumerGroupMsg wrapper
            ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(msg);
            assertEquals("events.user.created", cgMsg.getSubject());
            assertEquals("test message", new String(cgMsg.getData()));

            subscription.unsubscribe();
        }
    }

    private void createTestStream(JetStreamManagement jsm) {
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(TEST_STREAM)
                .subjects("*.>")
                .storageType(StorageType.Memory)
                .build();
            jsm.addStream(streamConfig);
        } catch (JetStreamApiException e) {
            // Stream might already exist, ignore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createKvBucket(KeyValueManagement kvm) {
        try {
            KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                .name("static-consumer-groups")
                .storageType(StorageType.Memory)
                .build();
            kvm.create(kvConfig);
        } catch (JetStreamApiException e) {
            // Bucket might already exist, ignore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanup(JetStreamManagement jsm, KeyValueManagement kvm) {
        try {
            jsm.deleteStream(TEST_STREAM);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        
        try {
            kvm.delete("static-consumer-groups");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}