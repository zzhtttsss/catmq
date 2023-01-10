package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
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
    private final ArrayBlockingQueue<ByteBuf> requestQueue = new ArrayBlockingQueue<>(1);

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
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            flush2File();
        }
        log.info(this.getServiceName() + " service end");
    }

    private void flush2File() {
        ByteBuf byteBuf;
        try {
            byteBuf = requestQueue.take();
            partitionSegmentStorage.flushLock.lock();
            File file = new File(StringUtil.concatString(partitionSegmentStorage.getPath(), File.separator,
                    StringUtil.offset2FileName(offset)));
            FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
            fileChannel.write(byteBuf.nioBuffer());
            fileChannel.force(true);
            partitionSegmentStorage.clearFlushedCache();
        } catch (InterruptedException e) {
            log.warn("{} interrupted, possibly by shutdown.", this.getServiceName());
            this.hasException = true;
//            return false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            partitionSegmentStorage.flushLock.unlock();
        }
    }
}
