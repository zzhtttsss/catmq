package org.catmq.storage.messageLog;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import io.netty.buffer.ByteBuf;
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static org.catmq.constant.FileConstant.RANDOM_ACCESS_FILE_READ_WRITE_MODE;

@Slf4j
public class MessageLog {

    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final AtomicIntegerFieldUpdater<MessageLog> WROTE_POSITION_UPDATER;
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    @Getter
    private String fileName;
    private File file;
    @Getter
    private int fileSize;
    @Getter
    private long offset;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    protected volatile int wrotePosition;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MessageLog.class, "wrotePosition");
    }

    public MessageLog(String fileName, int fileSize) throws IOException {
        this.file = new File(fileName);
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.offset = Long.parseLong(this.file.getName());
        boolean ok = false;
        try {
            this.fileChannel = new RandomAccessFile(this.file, RANDOM_ACCESS_FILE_READ_WRITE_MODE).getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file {}", this.fileName, e);

            throw e;
        } catch (IOException e) {
            log.error("Failed to map file {}", this.fileName, e);

            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public int appendMessageEntry(byte[] messageBytes, ByteBuf byteBuf, int beginIndex, int endIndex) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remainSize = this.fileSize - currentPos;
        if (messageBytes.length > remainSize) {
            byteBuf.writeBytes(messageBytes, beginIndex, remainSize);
            WROTE_POSITION_UPDATER.addAndGet(this, remainSize);
            return remainSize;
        }

        // TODO 目前只写入了消息体，没有加其他信息。
        byteBuf.writeBytes(messageBytes, beginIndex, endIndex - beginIndex);
        WROTE_POSITION_UPDATER.addAndGet(this, messageBytes.length);
        return 0;
    }

    public void flush() {
        this.mappedByteBuffer.force();
    }

    public void putAndFlush(ByteBuf byteBuf) {
        this.mappedByteBuffer.put(byteBuf.nioBuffer());
        flush();
    }

    public void warmMappedFile() {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MessageLog.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= flushLeastPagesWhenWarmMapedFile) {
                flush = i;
                mappedByteBuffer.force();
            }

            // prevent gc
            if (j % 1000 == 0) {
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        log.debug("Mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
        mappedByteBuffer.force();
        this.mlock();
    }

    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    public void setWritePosition(int index) {
        WROTE_POSITION_UPDATER.set(this, index);
    }

    public void resetWritePosition() {
        setWritePosition(0);
    }

    public void unlockMappedFile(){
        munlock();
    }

    private void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = CUtil.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.debug("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = CUtil.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), CUtil.MADV_WILLNEED);
            log.debug("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    private void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = CUtil.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.debug("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

}
