package org.catmq.storage.messageLog;

import lombok.Getter;

@Getter
public class MessageEntry {

    public static final String MSG_ID_DELIMITER = "_";
    public int length;

    public String msgId;

    public long chunkId;

    public byte[] message;

    public MessageEntry(String msgId, long chunkId, byte[] message) {
        this.msgId = msgId;
        this.chunkId = chunkId;
        this.message = message;
        this.length = message.length;
    }
}
