package org.catmq.collection;

import lombok.Getter;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Getter
public class TimerTaskList implements Delayed {
    //当前列表中包含的任务数
    private AtomicInteger taskCounter;
    // 列表的头结点
    private TimerTaskEntry root;
    // 过期时间
    private AtomicLong expiration = new AtomicLong(-1L);


    public TimerTaskList(AtomicInteger taskCounter) {
        this.taskCounter = taskCounter;
        this.root = new TimerTaskEntry(null, -1L);
        root.next = root;
        root.prev = root;
    }

    // 给当前槽设置过期时间
    public boolean setExpiration(Long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    public Long getExpiration() {
        return expiration.get();
    }

    // 用于遍历当前列表中的任务
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

    // 添加任务到列表中
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

    //获得当前任务剩余时间
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
        //包含一个任务
        private final DelayedMessageIndex delayedMessageIndex;
        // 任务的过期时间，此处的过期时间设置的过期间隔+系统当前时间（毫秒）
        private final Long expirationMs;

        // 当前任务属于哪一个列表
        private TimerTaskList list;
        // 当前任务的上一个任务，用双向列表连接
        private TimerTaskEntry prev;
        TimerTaskEntry next;


        public TimerTaskEntry(DelayedMessageIndex delayedMessageIndex, Long expirationMs) {
            this.delayedMessageIndex = delayedMessageIndex;
            this.expirationMs = expirationMs;
            // 传递进来任务TimerTask，并设置TimerTask的包装类
            if (delayedMessageIndex != null) {
                delayedMessageIndex.setTimerTaskEntry(this);
            }
        }

        // 任务的取消，就是判断任务TimerTask的Entry是否是当前任务
        public boolean cancel() {
            return delayedMessageIndex.getTimerTaskEntry() != this;
        }

        // 任务的移出
        public void remove() {
            TimerTaskList currentList = list;
            while (currentList != null) {
                currentList.remove(this);
                currentList = list;
            }
        }

        // 比较两个任务在列表中的位置，及那个先执行
        @Override
        public int compareTo(TimerTaskEntry that) {
            return Long.compare(expirationMs, that.expirationMs);
        }
    }
}


