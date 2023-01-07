package org.catmq.pipline;

import org.catmq.pipline.finisher.ExampleFinisher;
import org.catmq.pipline.preparer.ExamplePreparer;
import org.catmq.pipline.preparer.StorerPreparer;
import org.catmq.pipline.processor.ProduceProcessor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2BrokerRequest, SendMessage2BrokerResponse> SEND_MESSAGE_2_BROKER_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{StorerPreparer.StorerPreparerEnum.INSTANCE.getInstance(),
                    ExamplePreparer.ExamplePreparerEnum.INSTANCE.getInstance()},
                    ProduceProcessor.ProduceProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{ExampleFinisher.ExampleFinisherEnum.INSTANCE.getInstance()});

}
