package org.catmq.common;

import lombok.Getter;
import org.catmq.collection.RecyclableArrayList;

import java.util.List;

public class MessageEntryBatch implements AutoCloseable {
    private long batchSize;

    public static RecyclableArrayList.Recycler<MessageEntry> RECYCLER =
            new RecyclableArrayList.Recycler<>();

    @Getter
    private final RecyclableArrayList<MessageEntry> batch;

    public void put(MessageEntry messageEntry) {
        batch.add(messageEntry);
        batchSize += messageEntry.getTotalSize();
    }

    public void putAll(List<MessageEntry> list) {
        for (MessageEntry messageEntry : list) {
            batchSize += messageEntry.getTotalSize();
            batch.add(messageEntry);
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
        this.batch = RECYCLER.newInstance();
    }

    @Override
    public void close() {
        batch.recycle();
    }
}
