package org.catmq.storage.messageLog;

import lombok.extern.slf4j.Slf4j;
import org.catmq.collection.RecyclableArrayList;
import org.catmq.storage.ServiceThread;
import org.catmq.storer.Storer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class FlushMessageEntryService extends ServiceThread {

    public static final int BEGIN_OFFSET = 0;

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
                for (MessageEntry me : currentMessageEntries) {
                    if (!messageLog.appendMessageEntry(me)) {
                        // TODO 一批消息如果一部分在前一个messageLog中flush之后崩溃可能会出现消息重复。
                        messageLog.flush();
                        messageLog = Storer.STORER.messageLogStorage.getLastMessageLog(BEGIN_OFFSET);
                    }
                }
                messageLog.flush();
                for (MessageEntry me : currentMessageEntries) {
                    me.markFlushDone();
                }
                currentMessageEntries.recycle();
                lastFlushTime = System.currentTimeMillis();
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
