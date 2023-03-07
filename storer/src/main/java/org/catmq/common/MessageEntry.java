package org.catmq.common;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.protocol.definition.NumberedMessage;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static org.catmq.constant.CommonConstant.BYTES_LENGTH_OF_INT;
import static org.catmq.constant.CommonConstant.BYTES_LENGTH_OF_LONG;

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


    public MessageEntry(long segmentId, long entryId, byte[] message) {
        this.entryId = entryId;
        this.segmentId = segmentId;
        this.message = message;
        this.length = message.length;
    }

    public void markFlushDone() {
        this.waiter.countDown();
    }

    public int getTotalSize() {
        return this.getLength() + BYTES_LENGTH_OF_INT + 2 * BYTES_LENGTH_OF_LONG;
    }

    public void dump2ByteBuf(ByteBuf byteBuf) {
        byteBuf.writeBytes(conv2Bytes());
    }

    public NumberedMessage conv2NumberedMessage() {
        return NumberedMessage.newBuilder()
                .setSegmentId(segmentId)
                .setEntryId(entryId)
                .setBody(ByteString.copyFrom(message))
                .build();
    }

    public byte[] conv2Bytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(getTotalSize());
        // 1. put the length of the message and two Ids
        byteBuffer.putInt(length + 2 * BYTES_LENGTH_OF_LONG);
        // 2. put the segmentId
        byteBuffer.putLong(segmentId);
        // 3. put the entryId
        byteBuffer.putLong(entryId);
        // 4. put the message
        byteBuffer.put(message);
        return byteBuffer.array();
    }
}
