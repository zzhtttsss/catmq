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
    private Long tickMs = 1L;
    private Integer wheelSize = 20;
    private Long startMs = System.currentTimeMillis();
    //延迟队列
    private DelayQueue<TimerTaskList> delayQueue = new DelayQueue<>();
    private AtomicInteger taskCounter = new AtomicInteger(0);
    private TimingWheel timingWheel;
    private List<TimerTaskList.TimerTaskEntry> timeoutTaskList = new ArrayList<>();

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    // 用来执行时间轮的重新排列，及上一个槽中的任务列表被执行后，后面的槽中的任务列表移动
    private final Consumer<TimerTaskList.TimerTaskEntry> reinsert = (timerTaskEntry) -> addTimerTaskEntry(timerTaskEntry);

    public DelayedMessageTimer(Long tickMs, Integer wheelSize, Long startMs) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.timingWheel = new TimingWheel(
                tickMs,
                wheelSize,
                startMs,
                taskCounter,
                delayQueue
        );
    }

    public DelayedMessageTimer(Long startMs, long[] tickConfig, int[] wheelConfig) {
        this.tickMs = tickConfig[0];
        this.wheelSize = wheelConfig[0];
        this.startMs = startMs;
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

    // 可能会多个线程操作，所以需要加锁
    @Override
    public void add(DelayedMessageIndex delayedMessageIndex) {
        log.warn("add delayedMessageIndex: {}", delayedMessageIndex);
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskList.TimerTaskEntry(delayedMessageIndex, delayedMessageIndex.getExpireTimeMs()));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskList.TimerTaskEntry timerTaskEntry) {      // 往时间轮添加任务
        if (!timingWheel.add(timerTaskEntry)) {
            // 返回false并且任务未取消，则提交当前任务立即执行。
            if (!timerTaskEntry.cancel()) {
//                taskExecutor.submit(timerTaskEntry.timerTask);
                timeoutTaskList.add(timerTaskEntry);
            }
        }
    }


    // 向前驱动时间轮
    @Override
    public List<TimerTaskList.TimerTaskEntry> advanceClock(Long timeoutMs) {
        // 使用阻塞队列获取任务
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
                    // 驱动时间后，需要移动TimerTaskList到上一个槽或者从上一层移动到本层
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

