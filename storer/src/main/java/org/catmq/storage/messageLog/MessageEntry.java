package org.catmq.storage.messageLog;

import lombok.Getter;

import java.util.concurrent.CountDownLatch;

@Getter
public class MessageEntry {

    public static final String MSG_ID_DELIMITER = "_";

    public static final int WAIT_TIME = 1;
    private final int length;

    private final String msgId;

    private final long chunkId;

    private final byte[] message;

    private final CountDownLatch waiter = new CountDownLatch(WAIT_TIME);


    public MessageEntry(String msgId, long chunkId, byte[] message) {
        this.msgId = msgId;
        this.chunkId = chunkId;
        this.message = message;
        this.length = message.length;
    }

    public void markFlushDone() {
        this.waiter.countDown();
    }
}
