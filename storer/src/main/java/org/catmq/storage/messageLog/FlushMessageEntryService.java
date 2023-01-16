package org.catmq.storage.messageLog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.catmq.collection.RecyclableArrayList;
import org.catmq.thread.ServiceThread;
import org.catmq.storer.Storer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@Slf4j
public class FlushMessageEntryService extends ServiceThread {

    public static final int BEGIN_OFFSET = 0;

    private final BlockingQueue<MessageEntry> FlushMessageEntryQueue;

    private static final RecyclableArrayList.Recycler<MessageEntry> entryListRecycler =
            new RecyclableArrayList.Recycler<>();

    public FlushMessageEntryService() {
        // TODO 忙等待优化 BlockingMpscQueue
        this.FlushMessageEntryQueue = new ArrayBlockingQueue<>(STORER_CONFIG.getFlushMessageEntryQueueCapacity());
    }

    @Override
    public String getServiceName() {
        return MessageLogStorage.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());

        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 20 || FlushMessageEntryQueue.size() >= 10000) {
                if (FlushMessageEntryQueue.isEmpty()) {
                    continue;
                }

                MessageLog messageLog = Storer.STORER.messageLogStorage.getLatestMessageLog();
                RecyclableArrayList<MessageEntry> currentMessageEntries = entryListRecycler.newInstance();
                FlushMessageEntryQueue.drainTo(currentMessageEntries);

                ByteBuf byteBuf = Unpooled.directBuffer();
                for (MessageEntry me : currentMessageEntries) {
                    byte[] msgBytes = me.conv2Bytes();
                    int msgLength = msgBytes.length;
                    int nextBeginIndex = messageLog.appendMessageEntry(msgBytes, byteBuf, 0, msgLength);
                    if (nextBeginIndex != 0) {
                        log.debug("Need a new messageLog.");
                        // TODO 一批消息如果一部分在前一个messageLog中flush之后崩溃可能会出现消息重复。
                        // flush data to current MessageLog and change to next MessageLog.
                        messageLog.putAndFlush(byteBuf);
                        messageLog.unlockMappedFile();
                        byteBuf.clear();
                        messageLog = Storer.STORER.messageLogStorage.getLastMessageLog(BEGIN_OFFSET);
                        messageLog.appendMessageEntry(msgBytes, byteBuf, nextBeginIndex, msgLength);
                    }
                }

                messageLog.putAndFlush(byteBuf);
                byteBuf.release();
                for (MessageEntry me : currentMessageEntries) {
                    me.markFlushDone();
                }
                currentMessageEntries.recycle();
                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

    public void putMessageEntry2Queue(MessageEntry messageEntry) {
        try {
            FlushMessageEntryQueue.put(messageEntry);
        } catch (InterruptedException e) {
            log.warn("Interrupted! exception : {}", e.getMessage());
        }
    }
}
