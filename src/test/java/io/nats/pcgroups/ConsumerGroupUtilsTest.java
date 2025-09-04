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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerGroupUtilsTest {

    @Test
    void testComposeKey() {
        String result = ConsumerGroupUtils.composeKey("test-stream", "test-consumer-group");
        assertEquals("test-stream.test-consumer-group", result);
    }

    @Test
    void testGeneratePartitionFiltersBalanced() {
        List<String> members = Arrays.asList("member1", "member2", "member3");
        int maxMembers = 6;

        // Test member1 gets partitions 0, 1
        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member1");
        assertEquals(Arrays.asList("0.>", "1.>"), filters1);

        // Test member2 gets partitions 2, 3
        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member2");
        assertEquals(Arrays.asList("2.>", "3.>"), filters2);

        // Test member3 gets partitions 4, 5
        List<String> filters3 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member3");
        assertEquals(Arrays.asList("4.>", "5.>"), filters3);
    }

    @Test
    void testGeneratePartitionFiltersUneven() {
        List<String> members = Arrays.asList("member1", "member2", "member3");
        int maxMembers = 10;

        // Test member1 gets partitions 0, 1, 2 (plus remainder 0)
        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member1");
        assertEquals(Arrays.asList("0.>", "1.>", "2.>", "9.>"), filters1);

        // Test member2 gets partitions 3, 4, 5 (no remainder)
        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member2");
        assertEquals(Arrays.asList("3.>", "4.>", "5.>"), filters2);

        // Test member3 gets partitions 6, 7, 8 (no remainder)
        List<String> filters3 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "member3");
        assertEquals(Arrays.asList("6.>", "7.>", "8.>"), filters3);
    }

    @Test
    void testGeneratePartitionFiltersWithMappings() {
        List<MemberMapping> mappings = Arrays.asList(
            new MemberMapping("member1", Arrays.asList(0, 2, 4)),
            new MemberMapping("member2", Arrays.asList(1, 3, 5))
        );

        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            null, 6, mappings, "member1");
        assertEquals(Arrays.asList("0.>", "2.>", "4.>"), filters1);

        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            null, 6, mappings, "member2");
        assertEquals(Arrays.asList("1.>", "3.>", "5.>"), filters2);
    }

    @Test
    void testGeneratePartitionFiltersNonExistentMember() {
        List<String> members = Arrays.asList("member1", "member2");
        
        List<String> filters = ConsumerGroupUtils.generatePartitionFilters(
            members, 4, null, "nonexistent");
        assertTrue(filters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersEmptyMembers() {
        List<String> filters = ConsumerGroupUtils.generatePartitionFilters(
            Arrays.asList(), 4, null, "member1");
        assertTrue(filters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersMoreMembersThanMax() {
        List<String> members = Arrays.asList("m1", "m2", "m3", "m4", "m5");
        int maxMembers = 3;

        // Should only use first 3 members after sorting
        List<String> filters1 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "m1");
        assertEquals(Arrays.asList("0.>"), filters1);

        List<String> filters2 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "m2");
        assertEquals(Arrays.asList("1.>"), filters2);

        List<String> filters3 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "m3");
        assertEquals(Arrays.asList("2.>"), filters3);

        // m4 and m5 should get no partitions (excluded due to maxMembers limit)
        List<String> filters4 = ConsumerGroupUtils.generatePartitionFilters(
            members, maxMembers, null, "m4");
        assertTrue(filters4.isEmpty());
    }

    @Test
    void testDeduplicateStringList() {
        List<String> input = Arrays.asList("a", "b", "a", "c", "b", "d");
        List<String> result = ConsumerGroupUtils.deduplicateStringList(input);
        assertEquals(Arrays.asList("a", "b", "c", "d"), result);
    }

    @Test
    void testDeduplicateStringListEmpty() {
        List<String> input = Arrays.asList();
        List<String> result = ConsumerGroupUtils.deduplicateStringList(input);
        assertTrue(result.isEmpty());
    }

    @Test
    void testDeduplicateStringListNoDuplicates() {
        List<String> input = Arrays.asList("a", "b", "c");
        List<String> result = ConsumerGroupUtils.deduplicateStringList(input);
        assertEquals(input, result);
    }
}