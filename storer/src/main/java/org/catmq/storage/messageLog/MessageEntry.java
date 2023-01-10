package org.catmq.storage.messageLog;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.util.concurrent.CountDownLatch;

import static org.catmq.storage.messageLog.MessageLog.LENGTH_OF_INT;

@Getter
public class MessageEntry {

    public static final String MSG_ID_DELIMITER = "_";

    public static final int WAIT_TIME = 1;
    private final int length;

    private final String msgId;

    private final long segmentId;

    private final byte[] message;

    private final CountDownLatch waiter = new CountDownLatch(WAIT_TIME);


    public MessageEntry(String msgId, long segmentId, byte[] message) {
        this.msgId = msgId;
        this.segmentId = segmentId;
        this.message = message;
        this.length = message.length;
    }

    public void markFlushDone() {
        this.waiter.countDown();
    }

    public int getTotalSize() {
        return this.getLength() + LENGTH_OF_INT;
    }

    public void dump2ByteBuf(ByteBuf byteBuf) {
        byteBuf.writeInt(length);
        byteBuf.writeBytes(message);
    }
}
