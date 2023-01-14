package org.catmq.storage.messageLog;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

import static org.catmq.storage.messageLog.MessageLog.LENGTH_OF_INT;

@Slf4j
@Getter
public class MessageEntry {

    public static final String MSG_ID_DELIMITER = "_";

    public static final int WAIT_TIME = 1;
    private final int length;

    private final long entryId;

    private final long segmentId;

    private final byte[] message;

    @Getter
    @Setter
    private long offset;

    private final CountDownLatch waiter = new CountDownLatch(WAIT_TIME);


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
        return this.getLength() + LENGTH_OF_INT;
    }

    public void dump2ByteBuf(ByteBuf byteBuf) {
        log.info("length is {}", length);
        byteBuf.writeInt(length);
        log.info("bytebuf is {}", byteBuf.toString(CharsetUtil.UTF_8));
        log.info("int is {}", byteBuf.getInt(0));
        byteBuf.writeBytes(message);
    }
}
