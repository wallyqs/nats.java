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

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsJetStreamMetaData;

import java.time.Duration;

/**
 * Message wrapper for consumer group messages that strips the partition number from the subject
 */
public class ConsumerGroupMsg implements Message {
    private final Message msg;

    public ConsumerGroupMsg(Message msg) {
        this.msg = msg;
    }

    /**
     * Returns the message metadata
     */
    public NatsJetStreamMetaData metaData() {
        return msg.metaData();
    }

    /**
     * Returns the message body
     */
    @Override
    public byte[] getData() {
        return msg.getData();
    }

    /**
     * Returns a map of headers for a message
     */
    @Override
    public Headers getHeaders() {
        return msg.getHeaders();
    }

    /**
     * Returns a subject on which a message is published, with partition number stripped
     */
    @Override
    public String getSubject() {
        // strips the first token of the subject (it contains the partition number)
        String subject = msg.getSubject();
        int dotIndex = subject.indexOf('.');
        return dotIndex != -1 ? subject.substring(dotIndex + 1) : subject;
    }

    /**
     * Returns a reply subject for a message
     */
    @Override
    public String getReplyTo() {
        return msg.getReplyTo();
    }

    /**
     * Returns the connection this message was received on
     */
    @Override
    public io.nats.client.Connection getConnection() {
        return msg.getConnection();
    }

    /**
     * Returns the subscription this message was received on
     */
    @Override
    public io.nats.client.Subscription getSubscription() {
        return msg.getSubscription();
    }

    /**
     * Returns the SID for the subscription
     */
    @Override
    public String getSID() {
        return msg.getSID();
    }

    /**
     * Acknowledges a message
     * This tells the server that the message was successfully processed
     */
    public void ack() {
        msg.ack();
    }

    /**
     * Acknowledges a message and waits for ack from server
     */
    public void ackSync(Duration timeout) throws java.util.concurrent.TimeoutException, InterruptedException {
        msg.ackSync(timeout);
    }

    /**
     * Negatively acknowledges a message
     * This tells the server to redeliver the message
     */
    public void nak() {
        msg.nak();
    }

    /**
     * Negatively acknowledges a message with delay
     * This tells the server to redeliver the message after the given delay duration
     */
    public void nakWithDelay(Duration delay) {
        msg.nakWithDelay(delay);
    }

    /**
     * Negatively acknowledges a message with delay in milliseconds
     */
    public void nakWithDelay(long delayMillis) {
        msg.nakWithDelay(delayMillis);
    }

    /**
     * Tells the server that this message is being worked on
     * It resets the redelivery timer on the server
     */
    public void inProgress() {
        msg.inProgress();
    }

    /**
     * Tells the server to not redeliver this message, regardless of the value of MaxDeliver
     */
    public void term() {
        msg.term();
    }

    // Note: termWithReason is not available in the current nats.java version

    /**
     * Returns true if this is a JetStream message
     */
    @Override
    public boolean isJetStream() {
        return msg.isJetStream();
    }

    /**
     * Returns true if this is a status message
     */
    @Override
    public boolean isStatusMessage() {
        return msg.isStatusMessage();
    }

    /**
     * Returns the status of a status message
     */
    @Override
    public io.nats.client.support.Status getStatus() {
        return msg.getStatus();
    }

    // Note: isConsumed is not available in the current nats.java version
    
    /**
     * Returns the last acknowledgement type for this message
     */
    @Override
    public io.nats.client.impl.AckType lastAck() {
        return msg.lastAck();
    }

    /**
     * Returns whether the message is in UTF-8 mode
     */
    @Override
    public boolean isUtf8mode() {
        return msg.isUtf8mode();
    }

    /**
     * Returns whether the message has headers
     */
    @Override
    public boolean hasHeaders() {
        return msg.hasHeaders();
    }
}