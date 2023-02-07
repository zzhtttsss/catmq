package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.FileChannelWrapper;
import org.catmq.common.MessageEntry;
import org.catmq.thread.ServiceThread;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;


@Slf4j
public class FlushWriteCacheService extends ServiceThread {

    public static long lastSegmentOffset = 0L;

    private final SegmentStorage segmentStorage;

    private final SegmentFileManager segmentFileManager;

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

    public FlushWriteCacheService(SegmentStorage segmentStorage, SegmentFileManager segmentFileManager) {
        this.segmentStorage = segmentStorage;
        this.segmentFileManager = segmentFileManager;
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
        try {
            // Wait until there is a new writeCache.
            WriteCache writeCache = requestQueue.take();
            // Lock to avoid the writeCache to be modified by write threads.
            segmentStorage.flushLock.lock();

            try (FileChannelWrapper wrapper = segmentFileManager.getOrCreateSegmentFileByOffset(offset, true)) {
                FileChannel fileChannel = wrapper.getFileChannel();
                EntryPositionIndex entryPositionIndex = segmentStorage.getEntryPositionIndex();
                KeyValueStorage.Batch batch = entryPositionIndex.newBatch();

                writeCache.getCache().forEach((segmentId, map) -> {
                    log.warn("flush2File segmentId {}, size is {}", segmentId, map.size());
                    map.forEach((entryId, messageEntry) -> {
                        log.warn("flush2File segmentId {} entryId {}.", segmentId, entryId);
                        ByteBuf byteBuf = Unpooled.directBuffer(messageEntry.getTotalSize());
                        messageEntry.dump2ByteBuf(byteBuf);
                        try {
                            fileChannel.write(byteBuf.internalNioBuffer(0, byteBuf.readableBytes()));
                            entryPositionIndex.addPosition(batch, segmentId, entryId, lastSegmentOffset);
                            lastSegmentOffset += messageEntry.getTotalSize();
                        } catch (IOException e) {
                            this.hasException = true;
                            log.error("write offset {} file error or add index error {}.", offset, e);
                        }
                        byteBuf.release();
                    });
                });
                batch.flush();
                batch.close();
                fileChannel.force(true);
                fileChannel.close();
            }
            offset += writeCache.getCacheSize().get();
            segmentStorage.clearFlushedCache();
            log.info("success to flush index and segment.");
            return true;
        } catch (InterruptedException e) {
            log.warn("{} interrupted, possibly by shutdown.", this.getServiceName());
            this.hasException = true;
            return false;
        } catch (IOException e) {
            this.hasException = true;
            log.error("load offset {} file with error {}", offset, e);
            return false;
        } finally {
            segmentStorage.flushLock.unlock();
        }
    }

}
