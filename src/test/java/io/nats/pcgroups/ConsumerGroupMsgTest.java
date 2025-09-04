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

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.AckType;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerGroupMsgTest {

    @Test
    void testSubjectStripping() {
        Message mockMsg = new MockMessage("0.foo.bar.baz", "reply", "test data".getBytes());
        ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(mockMsg);
        
        // Should strip the partition number (first token)
        assertEquals("foo.bar.baz", cgMsg.getSubject());
    }

    @Test
    void testSubjectStrippingSingleToken() {
        Message mockMsg = new MockMessage("0", "reply", "test data".getBytes());
        ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(mockMsg);
        
        // If there's no dot, return the original subject
        assertEquals("0", cgMsg.getSubject());
    }

    @Test
    void testSubjectStrippingTwoTokens() {
        Message mockMsg = new MockMessage("5.events", "reply", "test data".getBytes());
        ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(mockMsg);
        
        assertEquals("events", cgMsg.getSubject());
    }

    @Test
    void testDelegation() {
        byte[] data = "test data".getBytes();
        Message mockMsg = new MockMessage("0.test.subject", "test.reply", data);
        ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(mockMsg);

        assertEquals(data, cgMsg.getData());
        assertEquals("test.reply", cgMsg.getReplyTo());
        assertTrue(cgMsg.isJetStream());
    }

    // Mock Message implementation for testing
    private static class MockMessage implements Message {
        private final String subject;
        private final String replyTo;
        private final byte[] data;

        public MockMessage(String subject, String replyTo, byte[] data) {
            this.subject = subject;
            this.replyTo = replyTo;
            this.data = data;
        }

        @Override
        public String getSubject() {
            return subject;
        }

        @Override
        public String getReplyTo() {
            return replyTo;
        }

        @Override
        public byte[] getData() {
            return data;
        }

        @Override
        public Headers getHeaders() {
            return null;
        }

        @Override
        public Connection getConnection() {
            return null;
        }

        @Override
        public Subscription getSubscription() {
            return null;
        }

        @Override
        public String getSID() {
            return null;
        }

        @Override
        public boolean isJetStream() {
            return true;
        }

        @Override
        public boolean isStatusMessage() {
            return false;
        }

        @Override
        public Status getStatus() {
            return null;
        }

        @Override
        public void ack() {}

        @Override
        public void ackSync(Duration timeout) {}

        @Override
        public void nak() {}

        @Override
        public void nakWithDelay(Duration nakDelay) {}

        @Override
        public void nakWithDelay(long nakDelayMillis) {}

        @Override
        public void inProgress() {}

        @Override
        public void term() {}

        @Override
        public NatsJetStreamMetaData metaData() {
            return null;
        }

        @Override
        public AckType lastAck() {
            return null;
        }

        @Override
        public boolean isUtf8mode() {
            return false;
        }

        @Override
        public boolean hasHeaders() {
            return false;
        }
    }
}