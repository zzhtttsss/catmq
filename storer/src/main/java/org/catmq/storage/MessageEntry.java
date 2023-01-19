package org.catmq.storage;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static org.catmq.constant.CommonConstant.BYTES_LENGTH_OF_INT;

@Slf4j
@Getter
public class MessageEntry {

    public static final int COUNT_DOWN_LATCH_WAIT_TIME = 1;
    private final int length;
    private final long entryId;
    private final long segmentId;
    private final byte[] message;

    @Setter
    private long offset;

    private final CountDownLatch waiter = new CountDownLatch(COUNT_DOWN_LATCH_WAIT_TIME);


    public MessageEntry(long entryId, long segmentId, byte[] message) {
        this.entryId = entryId;
        this.segmentId = segmentId;
        this.message = message;
        this.length = message.length;
    }

    public void markFlushDone() {
        this.waiter.countDown();
    }

    public int getTotalSize() {
        return this.getLength() + BYTES_LENGTH_OF_INT;
    }

    public void dump2ByteBuf(ByteBuf byteBuf) {
        byteBuf.writeBytes(conv2Bytes());
    }

    public byte[] conv2Bytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(getTotalSize());
        byteBuffer.putInt(length);
        byteBuffer.put(message);
        return byteBuffer.array();
    }
}
