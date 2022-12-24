package org.catmq.executor;

import org.catmq.finisher.Finisher;
import org.catmq.preparer.Preparer;
import org.catmq.processor.Processor;

public class TaskPlan<V, T> {

    private final Preparer[] preparers;

    private final Processor<V, T> processor;

    private final Finisher[] finishers;

    public TaskPlan(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {
        this.preparers = preparers;
        this.processor = processor;
        this.finishers = finishers;
    }

    public Preparer[] getPreparers() {
        return preparers;
    }

    public Processor<V, T> getProcessor() {
        return processor;
    }

    public Finisher[] getFinishers() {
        return finishers;
    }


}
