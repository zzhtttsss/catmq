package org.catmq.collection;

import lombok.Getter;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Getter
public class TimerTaskList implements Delayed {
    private final AtomicInteger taskCounter;
    private final TimerTaskEntry root;
    private final AtomicLong expiration = new AtomicLong(-1L);


    public TimerTaskList(AtomicInteger taskCounter) {
        this.taskCounter = taskCounter;
        this.root = new TimerTaskEntry(null, -1L);
        root.next = root;
        root.prev = root;
    }

    public boolean setExpiration(Long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    public Long getExpiration() {
        return expiration.get();
    }

    public synchronized void foreach(Consumer<DelayedMessageIndex> f) {
        TimerTaskEntry entry = root.next;
        while (entry != root) {
            TimerTaskEntry nextEntry = entry.next;
            if (!entry.cancel()) {
                f.accept(entry.delayedMessageIndex);
            }
            entry = nextEntry;
        }
    }

    public void add(TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            timerTaskEntry.remove();

            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.list == null) {
                        TimerTaskEntry tail = root.prev;
                        timerTaskEntry.next = root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.list = this;
                        tail.next = timerTaskEntry;
                        root.prev = timerTaskEntry;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    //移出任务
    synchronized void remove(TimerTaskEntry timerTaskEntry) {
        synchronized (timerTaskEntry) {
            if (timerTaskEntry.list == this) {
                timerTaskEntry.next.prev = timerTaskEntry.prev;
                timerTaskEntry.prev.next = timerTaskEntry.next;
                timerTaskEntry.next = null;
                timerTaskEntry.prev = null;
                timerTaskEntry.list = null;
                taskCounter.decrementAndGet();
            }
        }
    }

    public synchronized void flush(Consumer<TimerTaskEntry> f) {
        TimerTaskEntry head = root.next;
        while (head != root) {
            remove(head);
            f.accept(head);
            head = root.next;
        }
        expiration.set(-1L);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(Math.max(getExpiration() - System.currentTimeMillis(), 0), TimeUnit.MICROSECONDS);
    }

    @Override
    public int compareTo(Delayed d) {
        TimerTaskList other = (TimerTaskList) d;
        return Long.compare(getExpiration(), other.getExpiration());
    }

    @Getter
    public static class TimerTaskEntry implements Comparable<TimerTaskEntry> {
        private final DelayedMessageIndex delayedMessageIndex;
        private final Long expirationMs;
        private TimerTaskList list;
        private TimerTaskEntry prev;
        TimerTaskEntry next;


        public TimerTaskEntry(DelayedMessageIndex delayedMessageIndex, Long expirationMs) {
            this.delayedMessageIndex = delayedMessageIndex;
            this.expirationMs = expirationMs;
            if (delayedMessageIndex != null) {
                delayedMessageIndex.setTimerTaskEntry(this);
            }
        }

        public boolean cancel() {
            return delayedMessageIndex.getTimerTaskEntry() != this;
        }

        public void remove() {
            TimerTaskList currentList = list;
            while (currentList != null) {
                currentList.remove(this);
                currentList = list;
            }
        }

        @Override
        public int compareTo(TimerTaskEntry that) {
            return Long.compare(expirationMs, that.expirationMs);
        }
    }
}


