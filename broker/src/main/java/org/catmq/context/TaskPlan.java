package org.catmq.context;

import org.catmq.finisher.ExampleFinisher;
import org.catmq.finisher.Finisher;
import org.catmq.preparer.ExamplePreparer;
import org.catmq.preparer.Preparer;
import org.catmq.preparer.RegisterClientPreparer;
import org.catmq.preparer.StorerPreparer;
import org.catmq.processor.CreateTopicProcessor;
import org.catmq.processor.GetMessageProcessor;
import org.catmq.processor.Processor;
import org.catmq.processor.SendMessageProcessor;
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
