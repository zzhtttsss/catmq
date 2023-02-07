package org.catmq.collection;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TimingWheel {

    private final Long tickMs;
    private final Integer wheelSize;
    private final Long startMs;
    private final AtomicInteger taskCounter;
    private final DelayQueue<TimerTaskList> queue;

    private final Long interval;
    private final List<TimerTaskList> buckets;
    private Long currentTime;

    private TimingWheel overflowWheel = null;

    public TimingWheel(Long tickMs, Integer wheelSize, Long startMs, AtomicInteger taskCounter, DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.taskCounter = taskCounter;
        this.queue = queue;
        interval = tickMs * wheelSize;
        currentTime = startMs - (startMs % tickMs);

        buckets = new ArrayList<>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            buckets.add(new TimerTaskList(taskCounter));
        }
    }

    public TimingWheel(Long tickMs, Integer wheelSize, Long startMs, AtomicInteger taskCounter,
                       DelayQueue<TimerTaskList> queue, TimingWheel overflowWheel) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.overflowWheel = overflowWheel;
        interval = tickMs * wheelSize;
        currentTime = startMs - (startMs % tickMs);

        buckets = new ArrayList<>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            buckets.add(new TimerTaskList(taskCounter));
        }
    }

    public boolean add(TimerTaskList.TimerTaskEntry timerTaskEntry) {
        Long expiration = timerTaskEntry.getExpirationMs();
        if (timerTaskEntry.cancel()) {
            return false;
        } else if (expiration < currentTime + tickMs) {
            return false;
        } else if (expiration < currentTime + interval) {
            Long virtualId = expiration / tickMs;
            TimerTaskList bucket = buckets.get((int) (virtualId % wheelSize));
            bucket.add(timerTaskEntry);
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket);
            }
            return true;
        } else {
            if (overflowWheel == null) {
                log.warn("Should not arrive there, expiration is out of bounds.");
                return true;
            }
            return overflowWheel.add(timerTaskEntry);
        }
    }

    public void advanceClock(Long timeMs) {
        if (timeMs >= currentTime + tickMs) {
            currentTime = timeMs - (timeMs % tickMs);

            if (overflowWheel != null) {
                overflowWheel.advanceClock(currentTime);
            }
        }
    }
}