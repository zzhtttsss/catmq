package org.catmq;

import org.catmq.finisher.ExampleFinisher;
import org.catmq.pipline.Finisher;
import org.catmq.pipline.Preparer;
import org.catmq.pipline.Processor;
import org.catmq.preparer.ExamplePreparer;
import org.catmq.processor.WriteProcessor;
import org.catmq.protocol.service.SendMessage2StorerRequest;
import org.catmq.protocol.service.SendMessage2StorerResponse;

public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2StorerRequest, SendMessage2StorerResponse> SEND_MESSAGE_2_STORER_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{ExamplePreparer.ExamplePreparerEnum.INSTANCE.getInstance()},
                    WriteProcessor.WriteProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{ExampleFinisher.ExampleFinisherEnum.INSTANCE.getInstance()});
}
