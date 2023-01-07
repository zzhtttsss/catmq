package org.catmq.storage;

import java.util.concurrent.BlockingQueue;

public class MessageLogService extends ServiceThread {

//    private BlockingQueue<>

    @Override
    public String getServiceName() {
        return MessageLogStorage.class.getSimpleName();
    }

    @Override
    public void run() {


    }

    public void putMessageLogEntry2Queue() {

    }
}
