package org.catmq.broker.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.collection.DelayedMessageTimer;
import org.catmq.collection.TimerTaskList;
import org.catmq.thread.ServiceThread;

import java.util.List;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class ScheduleDelayedMessageService extends ServiceThread {

    @Getter
    private final DelayedMessageTimer delayedMessageTimer;

    private final BlockingQueue<List<TimerTaskList.TimerTaskEntry>> expireDelayedMessageQueue;

    public ScheduleDelayedMessageService(BlockingQueue<List<TimerTaskList.TimerTaskEntry>> expireDelayedMessageQueue) {
        this.expireDelayedMessageQueue = expireDelayedMessageQueue;
        this.delayedMessageTimer = new DelayedMessageTimer(System.currentTimeMillis(),
                new long[]{1000L, 1000L * 60, 1000L * 60 * 24}, new int[]{60, 60, 24});
    }

    @Override
    public String getServiceName() {
        return ScheduleDelayedMessageService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());
        long currentTime = System.currentTimeMillis();
        while (!this.isStopped()) {
            if (System.currentTimeMillis() < currentTime + 1000L) {
                continue;
            }
            List<TimerTaskList.TimerTaskEntry> timeoutDelayedMessageList = delayedMessageTimer.advanceClock(1000L);
            if (timeoutDelayedMessageList != null) {
                expireDelayedMessageQueue.add(timeoutDelayedMessageList);
            }
            currentTime += 1000L;
        }
        log.info("{} service end.", this.getServiceName());
    }
}
