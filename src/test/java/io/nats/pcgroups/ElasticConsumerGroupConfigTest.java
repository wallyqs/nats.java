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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ElasticConsumerGroupConfigTest {

    @Test
    void testIsInMembershipWithMembers() {
        List<String> members = Arrays.asList("member1", "member2", "member3");
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            10, "events.*", Arrays.asList(1), 1000, 50000, members, null);

        assertTrue(config.isInMembership("member1"));
        assertTrue(config.isInMembership("member2"));
        assertTrue(config.isInMembership("member3"));
        assertFalse(config.isInMembership("member4"));
    }

    @Test
    void testIsInMembershipWithMappings() {
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member1", Arrays.asList(0, 2, 4)),
            new MemberMapping("member2", Arrays.asList(1, 3, 5))
        );
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            6, "events.*", Arrays.asList(1), 0, 0, null, mappings);

        assertTrue(config.isInMembership("member1"));
        assertTrue(config.isInMembership("member2"));
        assertFalse(config.isInMembership("member3"));
    }

    @Test
    void testJsonSerialization() throws JsonProcessingException {
        List<String> members = Arrays.asList("member1", "member2");
        List<Integer> partitioningWildcards = Arrays.asList(1);
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            4, "events.*", partitioningWildcards, 1000, 50000, members, null);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(config);
        
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"events.*\""));
        assertTrue(json.contains("\"partitioning_wildcards\":[1]"));
        assertTrue(json.contains("\"max_buffered_msg\":1000"));
        assertTrue(json.contains("\"max_buffered_bytes\":50000"));
        assertTrue(json.contains("\"members\":[\"member1\",\"member2\"]"));
    }

    @Test
    void testJsonDeserialization() throws JsonProcessingException {
        String json = "{\"max_members\":6,\"filter\":\"events.*\",\"partitioning_wildcards\":[1],\"max_buffered_msg\":500,\"max_buffered_bytes\":25000,\"members\":[\"m1\",\"m2\"],\"member_mappings\":null}";
        
        ObjectMapper mapper = new ObjectMapper();
        ElasticConsumerGroupConfig config = mapper.readValue(json, ElasticConsumerGroupConfig.class);

        assertEquals(6, config.getMaxMembers());
        assertEquals("events.*", config.getFilter());
        assertEquals(Arrays.asList(1), config.getPartitioningWildcards());
        assertEquals(500, config.getMaxBufferedMsgs());
        assertEquals(25000, config.getMaxBufferedBytes());
        assertEquals(Arrays.asList("m1", "m2"), config.getMembers());
        assertNull(config.getMemberMappings());
    }

    @Test
    void testJsonSerializationWithMappings() throws JsonProcessingException {
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member1", Arrays.asList(0, 2)),
            new MemberMapping("member2", Arrays.asList(1, 3))
        );
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            4, "orders.*", Arrays.asList(1), 2000, 100000, null, mappings);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(config);
        
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"orders.*\""));
        assertTrue(json.contains("\"max_buffered_msg\":2000"));
        assertTrue(json.contains("\"max_buffered_bytes\":100000"));
        assertTrue(json.contains("\"member_mappings\""));
        assertTrue(json.contains("\"member\":\"member1\""));
        assertTrue(json.contains("\"partitions\":[0,2]"));
    }

    @Test
    void testGetters() {
        List<String> members = Arrays.asList("member1", "member2");
        List<Integer> partitioningWildcards = Arrays.asList(1, 2);
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member3", Arrays.asList(0, 1))
        );
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            8, "test.*.*", partitioningWildcards, 1500, 75000, members, mappings);

        assertEquals(8, config.getMaxMembers());
        assertEquals("test.*.*", config.getFilter());
        assertEquals(partitioningWildcards, config.getPartitioningWildcards());
        assertEquals(1500, config.getMaxBufferedMsgs());
        assertEquals(75000, config.getMaxBufferedBytes());
        assertEquals(members, config.getMembers());
        assertEquals(mappings, config.getMemberMappings());
    }

    @Test
    void testEqualsAndHashCode() {
        List<String> members = Arrays.asList("member1", "member2");
        List<Integer> partitioningWildcards = Arrays.asList(1);
        
        ElasticConsumerGroupConfig config1 = new ElasticConsumerGroupConfig(
            4, "events.*", partitioningWildcards, 1000, 50000, members, null);
        ElasticConsumerGroupConfig config2 = new ElasticConsumerGroupConfig(
            4, "events.*", partitioningWildcards, 1000, 50000, members, null);
        ElasticConsumerGroupConfig config3 = new ElasticConsumerGroupConfig(
            8, "events.*", partitioningWildcards, 1000, 50000, members, null);

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void testEmptyMembersAndMappings() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            4, "events.*", Arrays.asList(1), 0, 0, null, null);

        assertFalse(config.isInMembership("any-member"));
        assertNull(config.getMembers());
        assertNull(config.getMemberMappings());
    }

    @Test
    void testPartitioningWildcards() {
        List<Integer> wildcards = Arrays.asList(1, 3);
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
            4, "events.*.user.*", wildcards, 0, 0, null, null);

        assertEquals(wildcards, config.getPartitioningWildcards());
        assertEquals("events.*.user.*", config.getFilter());
    }
}