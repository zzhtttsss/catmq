package org.catmq.storage.messageLog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.catmq.collection.RecyclableArrayList;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;
import org.catmq.storer.Storer;
import org.catmq.thread.ServiceThread;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

/**
 * Service thread to flush {@link MessageEntry} to {@link MessageLog}.
 */
@Slf4j
public class FlushMessageEntryService extends ServiceThread {

    public static final int BEGIN_OFFSET = 0;

    /**
     * cache all pending {@link MessageEntry}.
     */
    private final BlockingQueue<MessageEntry> flushMessageEntryQueue;

    private static final RecyclableArrayList.Recycler<MessageEntry> entryListRecycler =
            new RecyclableArrayList.Recycler<>();

    private FlushMessageEntryService() {
        // TODO 忙等待优化 BlockingMpscQueue
        this.flushMessageEntryQueue = new ArrayBlockingQueue<>(STORER_CONFIG.getFlushMessageEntryQueueCapacity());
    }

    @Override
    public String getServiceName() {
        return MessageLogStorage.class.getSimpleName();
    }

    /**
     * Continue writing {@link MessageEntry} to the {@link MessageLog}.
     */
    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());

        long lastFlushTime = System.currentTimeMillis();
        while (!this.stopped) {
            // There are two situations that we need flush all messageEntry in queue to the disk:
            // 1. A certain amount of time has elapsed since the last flush.
            // 2. The number of messageEntry in the queue reaches a certain number.
            if (System.currentTimeMillis() - lastFlushTime > 20 || flushMessageEntryQueue.size() >= 10000) {
                if (flushMessageEntryQueue.isEmpty()) {
                    continue;
                }

                MessageLog messageLog = Storer.STORER.getMessageLogStorage().getLatestMessageLog();
                RecyclableArrayList<MessageEntry> currentMessageEntries = entryListRecycler.newInstance();
                // Move all messageEntry in the queue to an array.
                flushMessageEntryQueue.drainTo(currentMessageEntries);

                ByteBuf byteBuf = Unpooled.directBuffer();
                for (MessageEntry me : currentMessageEntries) {
                    byte[] msgBytes = me.conv2Bytes();
                    int msgLength = msgBytes.length;
                    int nextBeginIndex = messageLog.appendMessageEntry(msgBytes, byteBuf, 0, msgLength);
                    // It means that current messageLog do not have enough space, so we need to flush
                    // the current buffer to the messageLog and change to write the next messageLog.
                    if (nextBeginIndex != 0) {
                        log.debug("Need a new messageLog.");
                        // TODO: 一批消息如果一部分在前一个messageLog中flush之后崩溃可能会出现消息重复。
                        messageLog.putAndFlush(byteBuf);
                        // Unlock the full mapped file.
                        messageLog.unlockMappedFile();
                        byteBuf.clear();
                        messageLog = Storer.STORER.getMessageLogStorage().getLastMessageLog(BEGIN_OFFSET);
                        messageLog.appendMessageEntry(msgBytes, byteBuf, nextBeginIndex, msgLength);
                    }
                }

                messageLog.putAndFlush(byteBuf);
                byteBuf.release();
                // Mark each messageEntry as done to inform their requests.
                for (MessageEntry me : currentMessageEntries) {
                    me.markFlushDone();
                }
                currentMessageEntries.recycle();
                lastFlushTime = System.currentTimeMillis();
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

    @Deprecated
    public void putMessageEntry2Queue(MessageEntry messageEntry) {
        try {
            flushMessageEntryQueue.put(messageEntry);
        } catch (InterruptedException e) {
            log.warn("Interrupted! exception : {}", e.getMessage());
        }
    }

    public void batchPutMessageEntry2Queue(MessageEntryBatch messageEntryBatch) {
        try {
            for (MessageEntry me : messageEntryBatch.getBatch()) {
                flushMessageEntryQueue.put(me);
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted!", e);
        }
    }

    public enum FlushMessageEntryServiceEnum {
        INSTANCE;
        private final FlushMessageEntryService flushMessageEntryService;

        FlushMessageEntryServiceEnum() {
            flushMessageEntryService = new FlushMessageEntryService();
        }

        public FlushMessageEntryService getInstance() {
            return flushMessageEntryService;
        }
    }
}
