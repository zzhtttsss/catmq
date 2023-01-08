package org.catmq.storage;

public class MessageEntry {

    public static final String MSG_ID_DELIMITER = "_";

    public String msgId;

    public long chunkId;

    public byte[] message;

    public MessageEntry(String msgId, long chunkId, byte[] message) {
        this.msgId = msgId;
        this.chunkId = chunkId;
        this.message = message;
    }
}
