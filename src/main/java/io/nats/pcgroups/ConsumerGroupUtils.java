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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions for consumer groups
 */
public class ConsumerGroupUtils {
    // Configuration constants that match the Go implementation
    public static final Duration PULL_TIMEOUT = Duration.ofSeconds(3);
    public static final Duration ACK_WAIT = PULL_TIMEOUT.multipliedBy(2);
    public static final Duration CONSUMER_IDLE_TIMEOUT = PULL_TIMEOUT.multipliedBy(2);

    /**
     * Compose the consumer group's config key name
     */
    public static String composeKey(String streamName, String consumerGroupName) {
        return streamName + "." + consumerGroupName;
    }

    /**
     * Generate partition filters for a particular member of a consumer group
     * @param members List of members (for balanced distribution)
     * @param maxMembers Maximum number of members (partitions)
     * @param memberMappings Custom member mappings (alternative to balanced)
     * @param memberName The member name to generate filters for
     * @return List of partition filters for the member
     */
    public static List<String> generatePartitionFilters(List<String> members, 
                                                        int maxMembers, 
                                                        List<MemberMapping> memberMappings, 
                                                        String memberName) {
        if (members != null && !members.isEmpty()) {
            // Use balanced distribution
            Set<String> uniqueMembers = new LinkedHashSet<>(members);
            List<String> sortedMembers = new ArrayList<>(uniqueMembers);
            Collections.sort(sortedMembers);

            // Limit to maxMembers
            if (sortedMembers.size() > maxMembers) {
                sortedMembers = sortedMembers.subList(0, maxMembers);
            }

            int numMembers = sortedMembers.size();
            if (numMembers == 0) {
                return new ArrayList<>();
            }

            // Distribute partitions among members
            int numPer = maxMembers / numMembers;
            List<String> myFilters = new ArrayList<>();

            for (int i = 0; i < maxMembers; i++) {
                int memberIndex = i / numPer;

                if (i < (numMembers * numPer)) {
                    if (sortedMembers.get(memberIndex % numMembers).equals(memberName)) {
                        myFilters.add(String.format("%d.>", i));
                    }
                } else {
                    // Handle remainder partitions
                    int remainderIndex = (i - (numMembers * numPer)) % numMembers;
                    if (sortedMembers.get(remainderIndex).equals(memberName)) {
                        myFilters.add(String.format("%d.>", i));
                    }
                }
            }

            return myFilters;

        } else if (memberMappings != null && !memberMappings.isEmpty()) {
            // Use custom mappings
            return memberMappings.stream()
                    .filter(mapping -> memberName.equals(mapping.getMember()))
                    .flatMap(mapping -> mapping.getPartitions().stream())
                    .map(partition -> String.format("%d.>", partition))
                    .collect(Collectors.toList());
        }

        return new ArrayList<>();
    }

    /**
     * Deduplicate a list of strings while preserving order
     */
    public static List<String> deduplicateStringList(List<String> input) {
        return input.stream()
                .distinct()
                .collect(Collectors.toList());
    }
}