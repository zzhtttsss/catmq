package org.catmq.storage;

import lombok.extern.slf4j.Slf4j;
import org.catmq.collection.RecyclableArrayList;
import org.catmq.storer.Storer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class FlushMessageEntryService extends ServiceThread {


    private final BlockingQueue<MessageEntry> blockingQueue;

    private static final RecyclableArrayList.Recycler<MessageEntry> entryListRecycler =
            new RecyclableArrayList.Recycler<>();

    public FlushMessageEntryService() {
        // TODO 忙等待优化 BlockingMpscQueue 从config获取容量
        this.blockingQueue = new ArrayBlockingQueue<>(10000);
    }

    @Override
    public String getServiceName() {
        return MessageLogStorage.class.getSimpleName();
    }

    @Override
    public void run() {
        // TODO 当messageLog写满时切换下一个，采用rocketmq的方式，即由另一个线程提前准备好下一个并可以配置是否预热
        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 1000 || blockingQueue.size() >= 1000 * 0.5) {
                if (blockingQueue.isEmpty()) {
                    continue;
                }
                // TODO 向messageLog中写入entry，如果当前messageLog已满则从分配线程中获取到最新的messageLog再次写入。
                MessageLog messageLog = Storer.STORER.messageLogStorage.getLatestMessageLog();
                RecyclableArrayList<MessageEntry> currentMessageEntries = entryListRecycler.newInstance();
                blockingQueue.drainTo(currentMessageEntries);
                for (int i = 0; i < currentMessageEntries.size(); i++) {
                    messageLog.appendMessageEntry(currentMessageEntries.get(i));
                }
            }
        }
    }

    public void putMessageLogEntry2Queue(MessageEntry messageEntry) {
        try {
            blockingQueue.put(messageEntry);
        } catch (InterruptedException e) {
            log.warn("Interrupted! exception : {}", e.getMessage());
        }
    }
}
