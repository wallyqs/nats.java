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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for an elastic consumer group
 */
public class ElasticConsumerGroupConfig {
    @JsonProperty("max_members")
    private final int maxMembers;

    @JsonProperty("filter")
    private final String filter;

    @JsonProperty("partitioning_wildcards")
    private final List<Integer> partitioningWildcards;

    @JsonProperty("max_buffered_msg")
    private final long maxBufferedMsgs;

    @JsonProperty("max_buffered_bytes")
    private final long maxBufferedBytes;

    @JsonProperty("members")
    private final List<String> members;

    @JsonProperty("member_mappings")
    private final List<MemberMapping> memberMappings;

    public ElasticConsumerGroupConfig(@JsonProperty("max_members") int maxMembers,
                                    @JsonProperty("filter") String filter,
                                    @JsonProperty("partitioning_wildcards") List<Integer> partitioningWildcards,
                                    @JsonProperty("max_buffered_msg") long maxBufferedMsgs,
                                    @JsonProperty("max_buffered_bytes") long maxBufferedBytes,
                                    @JsonProperty("members") List<String> members,
                                    @JsonProperty("member_mappings") List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.filter = filter;
        this.partitioningWildcards = partitioningWildcards;
        this.maxBufferedMsgs = maxBufferedMsgs;
        this.maxBufferedBytes = maxBufferedBytes;
        this.members = members;
        this.memberMappings = memberMappings;
    }

    /**
     * Returns true if the member is in the current membership of the elastic consumer group
     */
    public boolean isInMembership(String name) {
        // Check member mappings first
        if (memberMappings != null) {
            for (MemberMapping mapping : memberMappings) {
                if (name.equals(mapping.getMember())) {
                    return true;
                }
            }
        }

        // Check members list
        return members != null && members.contains(name);
    }

    public int getMaxMembers() {
        return maxMembers;
    }

    public String getFilter() {
        return filter;
    }

    public List<Integer> getPartitioningWildcards() {
        return partitioningWildcards;
    }

    public long getMaxBufferedMsgs() {
        return maxBufferedMsgs;
    }

    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    public List<String> getMembers() {
        return members;
    }

    public List<MemberMapping> getMemberMappings() {
        return memberMappings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticConsumerGroupConfig that = (ElasticConsumerGroupConfig) o;
        return maxMembers == that.maxMembers &&
               maxBufferedMsgs == that.maxBufferedMsgs &&
               maxBufferedBytes == that.maxBufferedBytes &&
               Objects.equals(filter, that.filter) &&
               Objects.equals(partitioningWildcards, that.partitioningWildcards) &&
               Objects.equals(members, that.members) &&
               Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMembers, filter, partitioningWildcards, maxBufferedMsgs, maxBufferedBytes, members, memberMappings);
    }

    @Override
    public String toString() {
        return "ElasticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", filter='" + filter + '\'' +
                ", partitioningWildcards=" + partitioningWildcards +
                ", maxBufferedMsgs=" + maxBufferedMsgs +
                ", maxBufferedBytes=" + maxBufferedBytes +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}