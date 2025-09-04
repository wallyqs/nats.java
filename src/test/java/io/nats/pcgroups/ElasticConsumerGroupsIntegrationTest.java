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

import io.nats.client.*;
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ElasticConsumerGroups.
 * These tests require a running NATS server with JetStream enabled.
 * Set environment variable NATS_URL to run these tests (e.g., NATS_URL=nats://localhost:4222)
 */
@EnabledIfEnvironmentVariable(named = "NATS_URL", matches = ".*")
class ElasticConsumerGroupsIntegrationTest {

    private static final String TEST_STREAM = "test-elastic-stream";
    private static final String TEST_CONSUMER_GROUP = "test-elastic-cg";
    
    @Test
    void testElasticConsumerGroupCreation() throws Exception {
        String natsUrl = System.getenv("NATS_URL");
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            KeyValueManagement kvm = connection.keyValueManagement();

            // Clean up first
            cleanup(jsm, kvm, connection);

            // Create original stream
            createOriginalStream(jsm);

            // Create elastic consumer group
            ElasticConsumerGroupConfig config = ElasticConsumerGroups.createElastic(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, 4, "events.*", Arrays.asList(1), 1000, 50000);

            assertNotNull(config);
            assertEquals(4, config.getMaxMembers());
            assertEquals("events.*", config.getFilter());
            assertEquals(Arrays.asList(1), config.getPartitioningWildcards());
            assertEquals(1000, config.getMaxBufferedMsgs());
            assertEquals(50000, config.getMaxBufferedBytes());

            // Verify the consumer group stream was created
            String cgStreamName = ElasticConsumerGroups.composeCGStreamName(TEST_STREAM, TEST_CONSUMER_GROUP);
            StreamInfo cgStreamInfo = jsm.getStreamInfo(cgStreamName);
            assertNotNull(cgStreamInfo);
            assertEquals(cgStreamName, cgStreamInfo.getConfiguration().getName());

            // Test config retrieval
            ElasticConsumerGroupConfig retrievedConfig = ElasticConsumerGroups.getElasticConsumerGroupConfig(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP);
            assertEquals(config, retrievedConfig);
        }
    }

    @Test
    void testAddAndRemoveMembers() throws Exception {
        String natsUrl = System.getenv("NATS_URL");
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            KeyValueManagement kvm = connection.keyValueManagement();

            // Clean up and create
            cleanup(jsm, kvm, connection);
            createOriginalStream(jsm);

            // Create elastic consumer group
            ElasticConsumerGroups.createElastic(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, 4, "events.*", Arrays.asList(1), 0, 0);

            // Add members
            List<String> addedMembers = ElasticConsumerGroups.addMembers(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, Arrays.asList("member1", "member2"));

            assertEquals(Arrays.asList("member1", "member2"), addedMembers);

            // Verify members were added
            ElasticConsumerGroupConfig config = ElasticConsumerGroups.getElasticConsumerGroupConfig(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP);
            assertTrue(config.isInMembership("member1"));
            assertTrue(config.isInMembership("member2"));

            // Add more members
            List<String> moreMembers = ElasticConsumerGroups.addMembers(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, Arrays.asList("member3", "member4"));
            assertEquals(Arrays.asList("member3", "member4"), moreMembers);

            // Try to add beyond max members
            List<String> beyondMax = ElasticConsumerGroups.addMembers(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, Arrays.asList("member5"));
            assertTrue(beyondMax.isEmpty()); // Should not add beyond max

            // Remove members
            List<String> removedMembers = ElasticConsumerGroups.removeMembers(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, Arrays.asList("member2", "member4"));
            assertEquals(Arrays.asList("member2", "member4"), removedMembers);

            // Verify members were removed
            config = ElasticConsumerGroups.getElasticConsumerGroupConfig(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP);
            assertTrue(config.isInMembership("member1"));
            assertFalse(config.isInMembership("member2"));
            assertTrue(config.isInMembership("member3"));
            assertFalse(config.isInMembership("member4"));
        }
    }

    @Test
    void testElasticConsumerGroupDeletion() throws Exception {
        String natsUrl = System.getenv("NATS_URL");
        try (Connection connection = Nats.connect(natsUrl)) {
            JetStreamManagement jsm = connection.jetStreamManagement();
            KeyValueManagement kvm = connection.keyValueManagement();

            // Clean up and create
            cleanup(jsm, kvm, connection);
            createOriginalStream(jsm);

            // Create elastic consumer group
            ElasticConsumerGroups.createElastic(
                connection, TEST_STREAM, TEST_CONSUMER_GROUP, 4, "events.*", Arrays.asList(1), 0, 0);

            // Verify it exists
            String cgStreamName = ElasticConsumerGroups.composeCGStreamName(TEST_STREAM, TEST_CONSUMER_GROUP);
            assertDoesNotThrow(() -> jsm.getStreamInfo(cgStreamName));

            // Delete it
            ElasticConsumerGroups.deleteElastic(connection, TEST_STREAM, TEST_CONSUMER_GROUP);

            // Verify it's deleted
            assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(cgStreamName));
            assertThrows(IllegalArgumentException.class, () -> 
                ElasticConsumerGroups.getElasticConsumerGroupConfig(connection, TEST_STREAM, TEST_CONSUMER_GROUP));
        }
    }

    @Test
    void testStreamNameComposition() {
        String streamName = "my-stream";
        String consumerGroupName = "my-cg";
        String expected = "my-stream-cg-my-cg";
        
        String actual = ElasticConsumerGroups.composeCGStreamName(streamName, consumerGroupName);
        assertEquals(expected, actual);
    }

    @Test
    void testPartitionFilterGeneration() {
        // Test that partition filters work with elastic consumer groups
        List<String> members = Arrays.asList("member1", "member2");
        int maxMembers = 4;

        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member1");
        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member2");

        assertFalse(filters1.isEmpty());
        assertFalse(filters2.isEmpty());
        
        // Filters should be different for different members
        assertNotEquals(filters1, filters2);

        // All filters should follow the pattern "N.>"
        for (String filter : filters1) {
            assertTrue(filter.matches("\\d+\\.>"));
        }
        for (String filter : filters2) {
            assertTrue(filter.matches("\\d+\\.>"));
        }
    }

    private void createOriginalStream(JetStreamManagement jsm) {
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(TEST_STREAM)
                .subjects("events.*")
                .storageType(StorageType.Memory)
                .build();
            jsm.addStream(streamConfig);
        } catch (JetStreamApiException e) {
            // Stream might already exist, ignore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanup(JetStreamManagement jsm, KeyValueManagement kvm, Connection connection) {
        // Clean up consumer group stream
        try {
            String cgStreamName = ElasticConsumerGroups.composeCGStreamName(TEST_STREAM, TEST_CONSUMER_GROUP);
            jsm.deleteStream(cgStreamName);
        } catch (Exception e) {
            // Ignore cleanup errors
        }

        // Clean up original stream
        try {
            jsm.deleteStream(TEST_STREAM);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        
        // Clean up KV bucket
        try {
            kvm.delete("elastic-consumer-groups");
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}