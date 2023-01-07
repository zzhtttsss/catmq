package org.catmq.storage;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.catmq.constant.FileConstant.GB;

@Slf4j
public class MessageLogStorage {

    public final long maxMessageLogSize;

    public final String path;

    public final CopyOnWriteArrayList<MessageLog> messageLogs = new CopyOnWriteArrayList<>();





    public MessageLogStorage() {
        // TODO read config
        this.path = "./messageLog/";
        this.maxMessageLogSize = GB;

    }
}
