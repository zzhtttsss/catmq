package org.catmq.client.common;

import lombok.Getter;
import org.catmq.client.DefaultCatProducer;

import java.util.HashMap;
import java.util.Map;

@Getter
public class MessageEntry {

    private Map<String, String> properties;
    private byte[] body;


    public MessageEntry(byte[] body) {
        this.properties = new HashMap<>();
        this.body = body;
    }

    public static MessageEntryBuilder builder() {
        return new MessageEntryBuilder();
    }

    public static class MessageEntryBuilder {

        private Map<String, String> properties;
        private byte[] body;

        public MessageEntryBuilder() {
            this.properties = new HashMap<>();
        }

        public MessageEntryBuilder setProperties(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public MessageEntryBuilder setBody(byte[] body) {
            this.body = body;
            return this;
        }

        public MessageEntry build() {
            MessageEntry messageEntry = new MessageEntry(body);
            messageEntry.properties = this.properties;
            return messageEntry;
        }
    }



}
