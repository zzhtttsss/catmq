package org.catmq.collection;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TimingWheel {

    private Long tickMs;  //每一个槽表示的时间范围
    private Integer wheelSize; // 时间轮大小，即每一层时间轮的大小
    private Long startMs; // 系统的启动时间
    private AtomicInteger taskCounter;  // 当前层任务数
    private DelayQueue<TimerTaskList> queue; //延迟队列，用于从队列取每个任务列表

    private Long interval; //每一层时间轮代表的时间
    private List<TimerTaskList> buckets;  // 每一层的每一个槽中的时间任务列表
    private Long currentTime;  // 修正后的系统启动时间

    private TimingWheel overflowWheel = null;  // 上一层时间轮

    public TimingWheel(Long tickMs, Integer wheelSize, Long startMs, AtomicInteger taskCounter, DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.startMs = startMs;
        this.taskCounter = taskCounter;
        this.queue = queue;
        interval = tickMs * wheelSize;
        currentTime = startMs - (startMs % tickMs); //当前时间，往前推

        buckets = new ArrayList<>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            buckets.add(new TimerTaskList(taskCounter));  //创建每一个槽中的列表
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
        currentTime = startMs - (startMs % tickMs); //当前时间，往前推

        buckets = new ArrayList<>(wheelSize);
        for (int i = 0; i < wheelSize; i++) {
            buckets.add(new TimerTaskList(taskCounter));  //创建每一个槽中的列表
        }
    }

    // 添加任务
    public boolean add(TimerTaskList.TimerTaskEntry timerTaskEntry) {
        Long expiration = timerTaskEntry.getExpirationMs();

        Long thisTime = currentTime + tickMs;
        // 任务是否已经取消，取消则返回
        if (timerTaskEntry.cancel()) {
            return false;
            // 当前任务是否已经过期，如果过期则返回false，要立即执行
        } else if (expiration < currentTime + tickMs) {
            return false;
            // 判断当前任务能否在添加到当前时间轮
        } else if (expiration < currentTime + interval) {

            Long virtualId = expiration / tickMs;
            // 计算当前任务要分配在哪个槽中
            TimerTaskList bucket = buckets.get((int) (virtualId % wheelSize));

            bucket.add(timerTaskEntry);

            //更新槽的过期时间，添加入延迟队列
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket);
            }
            return true;
        } else {
            //添加任务到高层时间轮
            if (overflowWheel == null) {
                log.warn("Should not arrive there, expiration is out of bounds.");
                return true;
            }
            return overflowWheel.add(timerTaskEntry);
        }
    }

    // 向前驱动时间
    public void advanceClock(Long timeMs) {
        if (timeMs >= currentTime + tickMs) {
            currentTime = timeMs - (timeMs % tickMs);

            if (overflowWheel != null) {
                overflowWheel.advanceClock(currentTime);
            }
        }
    }
}