package org.catmq.collection;

import java.util.List;

public interface Timer {
    void add(DelayedMessageIndex delayedMessageIndex);

    List<TimerTaskList.TimerTaskEntry> advanceClock(Long timeoutMs) throws Exception;

    int size();

    void shutdown();
}
