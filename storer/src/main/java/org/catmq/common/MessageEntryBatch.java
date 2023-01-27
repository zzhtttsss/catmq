package org.catmq.common;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class MessageEntryBatch {
    private long batchSize;

    @Getter
    private long batchSegmentId;

    @Getter
    private final List<MessageEntry> batch;

    public void put(MessageEntry messageEntry) {
        batchSegmentId = messageEntry.getSegmentId();
        batch.add(messageEntry);
        batchSize += messageEntry.getTotalSize();
    }

    public void putAll(List<MessageEntry> list) {
        for (MessageEntry messageEntry : list) {
            put(messageEntry);
        }
    }


    public MessageEntry get(int index) {
        return batch.get(index);
    }

    public boolean isEmpty() {
        return batch.isEmpty();
    }

    public long getTotalSize() {
        return batchSize;
    }

    public MessageEntryBatch() {
        this.batchSize = 0;
        this.batch = new ArrayList<>();
    }

}
