package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.ServiceThread;
import org.catmq.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
public class FlushWriteCacheService extends ServiceThread {

    private final PartitionSegmentStorage partitionSegmentStorage;

    @Getter
    private final ArrayBlockingQueue<WriteCache> requestQueue = new ArrayBlockingQueue<>(1);

    private long offset = 0;

    private volatile boolean hasException = false;

    public FlushWriteCacheService(PartitionSegmentStorage partitionSegmentStorage) {
        this.partitionSegmentStorage = partitionSegmentStorage;
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

    private boolean flush2File() {
        WriteCache writeCache;
        String fileName = StringUtil.concatString(partitionSegmentStorage.getPath(), File.separator,
                StringUtil.offset2FileName(offset));
        try {
            writeCache = requestQueue.take();
            partitionSegmentStorage.flushLock.lock();
            File file = new File(fileName);
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            EntryOffsetIndex entryOffsetIndex = partitionSegmentStorage.getEntryOffsetIndex();
            KeyValueStorage.Batch batch = entryOffsetIndex.newBatch();
            writeCache.getCache().forEach((segmentId, map) -> {
                map.forEach((entryId, messageEntry) -> {
                    ByteBuf byteBuf = Unpooled.directBuffer(messageEntry.getTotalSize());
                    messageEntry.dump2ByteBuf(byteBuf);
                    try {
                        fileChannel.write(byteBuf.nioBuffer());
                        entryOffsetIndex.addLocation(batch, segmentId, entryId, messageEntry.getOffset());
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
            partitionSegmentStorage.clearFlushedCache();
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
            partitionSegmentStorage.flushLock.unlock();
        }
    }

}
