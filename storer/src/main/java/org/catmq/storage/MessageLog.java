package org.catmq.storage;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.util.CUtil;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

@Slf4j
public class MessageLog {

    public static final int OS_PAGE_SIZE = 1024 * 4;

    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

    protected static final AtomicIntegerFieldUpdater<MessageLog> WROTE_POSITION_UPDATER;

    protected static final AtomicIntegerFieldUpdater<MessageLog> COMMITTED_POSITION_UPDATER;

    protected static final AtomicIntegerFieldUpdater<MessageLog> FLUSHED_POSITION_UPDATER;

    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    @Getter
    public String fileName;

    public File file;

    @Getter
    public int fileSize;

    @Getter
    public long offset;

    public FileChannel fileChannel;

    public MappedByteBuffer mappedByteBuffer;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageLog.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageLog.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageLog.class, "flushedPosition");
    }

    public MessageLog(String fileName, int fileSize) throws IOException {
        this.file = new File(fileName);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.offset = Long.parseLong(this.file.getName());
        boolean ok = false;
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);

            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);

            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public void appendMessageEntry(MessageEntry messageEntry) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);



    }

    public void warmMappedFile() {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MessageLog.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
//            if (type == FlushDiskType.SYNC_FLUSH) {
            if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= flushLeastPagesWhenWarmMapedFile) {
                flush = i;
                mappedByteBuffer.force();
            }
//            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
//        if (type == FlushDiskType.SYNC_FLUSH) {
        log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
        mappedByteBuffer.force();
//        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = CUtil.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = CUtil.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), CUtil.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = CUtil.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

}
