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
 * Mapping of a member to specific partitions
 */
public class MemberMapping {
    @JsonProperty("member")
    private final String member;

    @JsonProperty("partitions")
    private final List<Integer> partitions;

    public MemberMapping(@JsonProperty("member") String member, 
                        @JsonProperty("partitions") List<Integer> partitions) {
        this.member = member;
        this.partitions = partitions;
    }

    public String getMember() {
        return member;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberMapping that = (MemberMapping) o;
        return Objects.equals(member, that.member) && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(member, partitions);
    }

    @Override
    public String toString() {
        return "MemberMapping{" +
                "member='" + member + '\'' +
                ", partitions=" + partitions +
                '}';
    }
}