package org.catmq.storage.messageLog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
        this.blockingQueue = new ArrayBlockingQueue<>(100000);
    }

    @Override
    public String getServiceName() {
        return MessageLogStorage.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        // TODO 当messageLog写满时切换下一个，采用rocketmq的方式，即由另一个线程提前准备好下一个并可以配置是否预热
        long lastFlushTime = System.currentTimeMillis();

        while (!this.stopped) {
            if (System.currentTimeMillis() - lastFlushTime > 20 || blockingQueue.size() >= 10000) {
                if (blockingQueue.isEmpty()) {
                    continue;
                }

                // TODO 向messageLog中写入entry，如果当前messageLog已满则从分配线程中获取到最新的messageLog再次写入。
                MessageLog messageLog = Storer.STORER.messageLogStorage.getLatestMessageLog();
                RecyclableArrayList<MessageEntry> currentMessageEntries = entryListRecycler.newInstance();
                blockingQueue.drainTo(currentMessageEntries);

                ByteBuf byteBuf = Unpooled.directBuffer();
                for (MessageEntry me : currentMessageEntries) {
                    byte[] msgBytes = me.conv2Bytes();
                    int msgLength = msgBytes.length;
                    int nextBeginIndex = messageLog.appendMessageEntry(msgBytes, byteBuf, 0, msgLength);
                    if (nextBeginIndex != 0) {
                        log.debug("need a new messageLog.");
                        // TODO 一批消息如果一部分在前一个messageLog中flush之后崩溃可能会出现消息重复。
                        // flush data to current MessageLog and change to next MessageLog.
                        messageLog.putAndFlush(byteBuf);
                        byteBuf.clear();
                        messageLog = Storer.STORER.messageLogStorage.getLastMessageLog(BEGIN_OFFSET);
                        log.debug("new messageLog offset is {}, index is {}", messageLog.getOffset(), MessageLog.WROTE_POSITION_UPDATER.get(messageLog));
                        log.debug("filename is {}", messageLog.getFileName());
                        messageLog.appendMessageEntry(msgBytes, byteBuf, nextBeginIndex, msgLength);
                        log.debug("new messageLog, index is {}", MessageLog.WROTE_POSITION_UPDATER.get(messageLog));
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
        log.info(this.getServiceName() + " service end");
    }

    public void putMessageEntry2Queue(MessageEntry messageEntry) {
        try {
            blockingQueue.put(messageEntry);
        } catch (InterruptedException e) {
            log.warn("Interrupted! exception : {}", e.getMessage());
        }
    }
}
