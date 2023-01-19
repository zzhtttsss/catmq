package org.catmq.pipline;

import org.catmq.pipline.finisher.ExampleFinisher;
import org.catmq.pipline.preparer.ExamplePreparer;
import org.catmq.pipline.preparer.RegisterClientPreparer;
import org.catmq.pipline.preparer.StorerPreparer;
import org.catmq.pipline.processor.CreateTopicProcessor;
import org.catmq.pipline.processor.GetMessageProcessor;
import org.catmq.pipline.processor.SendMessageProcessor;
import org.catmq.protocol.service.*;

public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2BrokerRequest, SendMessage2BrokerResponse> SEND_MESSAGE_2_BROKER_TASK_PLAN =
            new TaskPlan<>(
                    new Preparer[]{
                            StorerPreparer.StorerPreparerEnum.INSTANCE.getInstance(),
                            ExamplePreparer.ExamplePreparerEnum.INSTANCE.getInstance()
                    },
                    SendMessageProcessor.ProduceProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{
                            ExampleFinisher.ExampleFinisherEnum.INSTANCE.getInstance()
                    }
            );

    public static final TaskPlan<CreateTopicRequest, CreateTopicResponse> CREATE_TOPIC_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{},
                    CreateTopicProcessor.CreateTopicProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{}
            );
    public static final TaskPlan<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse>
            GET_MESSAGE_FROM_BROKER_TASK_PLAN = new TaskPlan<>(new Preparer[]{
            RegisterClientPreparer.RegisterClientPreparerEnum.INSTANCE.getInstance()
    },
            GetMessageProcessor.GetMessageProcessorEnum.INSTANCE.getInstance(),
            new Finisher[]{}
    );
}
