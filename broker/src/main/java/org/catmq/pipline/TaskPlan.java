package org.catmq.pipline;

import org.catmq.pipline.preparer.RegisterClientPreparer;
import org.catmq.pipline.processor.*;
import org.catmq.protocol.service.*;

public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2BrokerRequest, SendMessage2BrokerResponse> SEND_MESSAGE_2_BROKER_TASK_PLAN =
            new TaskPlan<>(
                    new Preparer[]{},
                    SendMessageProcessor.ProduceProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{}
            );

    public static final TaskPlan<CreateTopicRequest, CreateTopicResponse> CREATE_TOPIC_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{},
                    CreateTopicProcessor.CreateTopicProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{}
            );

    public static final TaskPlan<CreatePartitionRequest, CreatePartitionResponse> CREATE_PARTITION_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{},
                    CreatePartitionProcessor.CreatePartitionProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{}
            );

    public static final TaskPlan<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse>
            GET_MESSAGE_FROM_BROKER_TASK_PLAN = new TaskPlan<>(new Preparer[]{
            RegisterClientPreparer.RegisterClientPreparerEnum.INSTANCE.getInstance()
    },
            GetMessageProcessor.GetMessageProcessorEnum.INSTANCE.getInstance(),
            new Finisher[]{}
    );
    public static final TaskPlan<SubscribeRequest, SubscribeResponse> SUBSCRIBE_FROM_CONSUMER =
            new TaskPlan<>(
                    new Preparer[]{
                            RegisterClientPreparer.RegisterClientPreparerEnum.INSTANCE.getInstance()
                    },
                    SubscribeProcessor.SubscribeProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{}
            );
}
