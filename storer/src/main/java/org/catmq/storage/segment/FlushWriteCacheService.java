package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.MessageEntry;
import org.catmq.thread.ServiceThread;
import org.catmq.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
public class FlushWriteCacheService extends ServiceThread {

    private final SegmentStorage segmentStorage;

    /**
     * Receive the writeCache to be flushed.
     * Because It is guaranteed that only one write thread can do swap and flush at a time, the
     * capacity of the queue should be 1.
     */
    @Getter
    private final ArrayBlockingQueue<WriteCache> requestQueue = new ArrayBlockingQueue<>(1);

    /**
     * Represent the offset which is the beginning of the next segment file.
     */
    private long offset = 0;

    private volatile boolean hasException = false;

    public FlushWriteCacheService(SegmentStorage segmentStorage) {
        this.segmentStorage = segmentStorage;
    }

    @Override
    public String getServiceName() {
        return FlushWriteCacheService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started.");

        while (!this.isStopped() && flush2File()) {

        }

        log.info(this.getServiceName() + " service end.");
    }

    /**
     * Flush the {@link WriteCache} to the disk.
     * All {@link MessageEntry} in the {@link WriteCache} are flushed to a new segment file.
     * The position of each {@link MessageEntry} in the file is also inserted into the database
     * as an index.
     *
     * @return whether the service need continue running.
     */
    private boolean flush2File() {
        WriteCache writeCache;
        String fileName = StringUtil.concatString(segmentStorage.getPath(), File.separator,
                StringUtil.offset2FileName(offset));
        try {
            // Wait until there is a new writeCache.
            writeCache = requestQueue.take();
            // Lock to avoid the writeCache to be modified by write threads.
            segmentStorage.flushLock.lock();

            File file = new File(fileName);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            EntryPositionIndex entryPositionIndex = segmentStorage.getEntryPositionIndex();
            KeyValueStorage.Batch batch = entryPositionIndex.newBatch();

            writeCache.getCache().forEach((segmentId, map) -> {
                map.forEach((entryId, messageEntry) -> {
                    ByteBuf byteBuf = Unpooled.directBuffer(messageEntry.getTotalSize());
                    messageEntry.dump2ByteBuf(byteBuf);
                    try {
                        fileChannel.write(byteBuf.internalNioBuffer(0, byteBuf.readableBytes()));
                        entryPositionIndex.addPosition(batch, segmentId, entryId, messageEntry.getOffset());
                    } catch (IOException e) {
                        this.hasException = true;
                        log.error("write file " + fileName + " error or add index error.", e);
                    }
                    byteBuf.release();
                });
            });
            batch.flush();
            batch.close();
            fileChannel.force(true);
            fileChannel.close();
            offset += writeCache.getCacheSize().get();
            log.warn("success to flush index and segment.");
            segmentStorage.clearFlushedCache();
            return true;
        } catch (InterruptedException e) {
            log.warn("{} interrupted, possibly by shutdown.", this.getServiceName());
            this.hasException = true;
            return false;
        } catch (IOException e) {
            this.hasException = true;
            log.error("load file " + fileName + " error", e);
            return false;
        } finally {
            segmentStorage.flushLock.unlock();
        }
    }

}
