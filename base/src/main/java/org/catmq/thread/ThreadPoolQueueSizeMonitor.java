package org.catmq.thread;

import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolQueueSizeMonitor implements ThreadPoolStatusMonitor {
    private final int maxQueueCapacity;

    public ThreadPoolQueueSizeMonitor(int maxQueueCapacity) {
        this.maxQueueCapacity = maxQueueCapacity;
    }

    @Override
    public String describe() {
        return "queueSize";
    }

    @Override
    public double value(ThreadPoolExecutor executor) {
        return executor.getQueue().size();
    }

    @Override
    public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
        return value > maxQueueCapacity * 0.85;
    }
}
