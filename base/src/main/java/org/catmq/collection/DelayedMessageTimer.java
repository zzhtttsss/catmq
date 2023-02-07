package org.catmq.collection;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

@Slf4j
public class DelayedMessageTimer implements Timer {
    private DelayQueue<TimerTaskList> delayQueue = new DelayQueue<>();
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final TimingWheel timingWheel;
    private final List<TimerTaskList.TimerTaskEntry> timeoutTaskList = new ArrayList<>();

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    private final Consumer<TimerTaskList.TimerTaskEntry> reinsert = (timerTaskEntry) -> addTimerTaskEntry(timerTaskEntry);

    public DelayedMessageTimer(Long startMs, long[] tickConfig, int[] wheelConfig) {
        TimingWheel previous = null;
        TimingWheel current;
        for (int i = wheelConfig.length - 1; i >= 0; i--) {
            current = new TimingWheel(
                    tickConfig[i],
                    wheelConfig[i],
                    startMs,
                    taskCounter,
                    delayQueue,
                    previous
            );
            previous = current;
        }
        this.timingWheel = previous;
    }

    @Override
    public void add(DelayedMessageIndex delayedMessageIndex) {
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskList.TimerTaskEntry(delayedMessageIndex, delayedMessageIndex.getExpireTimeMs()));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskList.TimerTaskEntry timerTaskEntry) {      // 往时间轮添加任务
        if (!timingWheel.add(timerTaskEntry)) {
            if (!timerTaskEntry.cancel()) {
                timeoutTaskList.add(timerTaskEntry);
            }
        }
    }


    @Override
    public List<TimerTaskList.TimerTaskEntry> advanceClock(Long timeoutMs) {
        TimerTaskList bucket = null;
        try {
            bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted!", e);
        }
        if (bucket != null) {
            writeLock.lock();
            try {
                while (bucket != null) {
                    timingWheel.advanceClock(bucket.getExpiration());
                    bucket.flush(reinsert);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            List<TimerTaskList.TimerTaskEntry> list = new ArrayList<>(timeoutTaskList);
            timeoutTaskList.clear();
            return list;
        } else {
            return null;
        }
    }

    @Override
    public int size() {
        return taskCounter.get();
    }

    @Override
    public void shutdown() {
    }
}

