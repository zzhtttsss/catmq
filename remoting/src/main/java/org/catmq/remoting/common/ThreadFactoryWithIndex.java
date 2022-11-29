package org.catmq.remoting.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryWithIndex implements ThreadFactory {
    private final String namePrefix;
    private final AtomicInteger threadIndex = new AtomicInteger(0);

    public ThreadFactoryWithIndex(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, this.namePrefix + this.threadIndex.incrementAndGet());
    }

}
