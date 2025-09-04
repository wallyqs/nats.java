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

class StaticConsumerGroupConfigTest {

    @Test
    void testIsInMembershipWithMembers() {
        List<String> members = Arrays.asList("member1", "member2", "member3");
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            10, "*.>", members, null);

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
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            6, "*.>", null, mappings);

        assertTrue(config.isInMembership("member1"));
        assertTrue(config.isInMembership("member2"));
        assertFalse(config.isInMembership("member3"));
    }

    @Test
    void testIsInMembershipWithBoth() {
        List<String> members = Arrays.asList("member1");
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member2", Arrays.asList(0, 1))
        );
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            4, "*.>", members, mappings);

        // Should check both members and mappings
        assertTrue(config.isInMembership("member1"));
        assertTrue(config.isInMembership("member2"));
        assertFalse(config.isInMembership("member3"));
    }

    @Test
    void testJsonSerialization() throws JsonProcessingException {
        List<String> members = Arrays.asList("member1", "member2");
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            4, "events.>", members, null);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(config);
        
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"events.>\""));
        assertTrue(json.contains("\"members\":[\"member1\",\"member2\"]"));
    }

    @Test
    void testJsonDeserialization() throws JsonProcessingException {
        String json = "{\"max_members\":6,\"filter\":\"*.>\",\"members\":[\"m1\",\"m2\"],\"member_mappings\":null}";
        
        ObjectMapper mapper = new ObjectMapper();
        StaticConsumerGroupConfig config = mapper.readValue(json, StaticConsumerGroupConfig.class);

        assertEquals(6, config.getMaxMembers());
        assertEquals("*.>", config.getFilter());
        assertEquals(Arrays.asList("m1", "m2"), config.getMembers());
        assertNull(config.getMemberMappings());
    }

    @Test
    void testJsonSerializationWithMappings() throws JsonProcessingException {
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member1", Arrays.asList(0, 2)),
            new MemberMapping("member2", Arrays.asList(1, 3))
        );
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            4, "events.>", null, mappings);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(config);
        
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"events.>\""));
        assertTrue(json.contains("\"member_mappings\""));
        assertTrue(json.contains("\"member\":\"member1\""));
        assertTrue(json.contains("\"partitions\":[0,2]"));
    }

    @Test
    void testGetters() {
        List<String> members = Arrays.asList("member1", "member2");
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member3", Arrays.asList(0, 1))
        );
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
            8, "test.>", members, mappings);

        assertEquals(8, config.getMaxMembers());
        assertEquals("test.>", config.getFilter());
        assertEquals(members, config.getMembers());
        assertEquals(mappings, config.getMemberMappings());
    }

    @Test
    void testEqualsAndHashCode() {
        List<String> members = Arrays.asList("member1", "member2");
        StaticConsumerGroupConfig config1 = new StaticConsumerGroupConfig(
            4, "events.>", members, null);
        StaticConsumerGroupConfig config2 = new StaticConsumerGroupConfig(
            4, "events.>", members, null);
        StaticConsumerGroupConfig config3 = new StaticConsumerGroupConfig(
            8, "events.>", members, null);

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
        assertEquals(config1.hashCode(), config2.hashCode());
    }
}