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
 * Configuration for a static consumer group
 */
public class StaticConsumerGroupConfig {
    @JsonProperty("max_members")
    private final int maxMembers;

    @JsonProperty("filter")
    private final String filter;

    @JsonProperty("members")
    private final List<String> members;

    @JsonProperty("member_mappings")
    private final List<MemberMapping> memberMappings;

    public StaticConsumerGroupConfig(@JsonProperty("max_members") int maxMembers,
                                   @JsonProperty("filter") String filter,
                                   @JsonProperty("members") List<String> members,
                                   @JsonProperty("member_mappings") List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.filter = filter;
        this.members = members;
        this.memberMappings = memberMappings;
    }

    /**
     * Returns true if the member is in the current membership of the static consumer group
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
        StaticConsumerGroupConfig that = (StaticConsumerGroupConfig) o;
        return maxMembers == that.maxMembers &&
               Objects.equals(filter, that.filter) &&
               Objects.equals(members, that.members) &&
               Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMembers, filter, members, memberMappings);
    }

    @Override
    public String toString() {
        return "StaticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", filter='" + filter + '\'' +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}