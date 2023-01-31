package org.catmq.pipline;

import org.catmq.pipline.processor.CreateSegmentProcessor;
import org.catmq.pipline.processor.ReadProcessor;
import org.catmq.pipline.processor.WriteProcessor;
import org.catmq.protocol.service.*;

/**
 * Represent how to process a grpc request.
 *
 * @param preparers all {@link Preparer} need to be done
 * @param processor processor that handle main logic
 * @param finishers all {@link Finisher} need to be done
 * @param <V>       class of the grpc request
 * @param <T>       class of the grpc response
 */
public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2StorerRequest, SendMessage2StorerResponse> SEND_MESSAGE_2_STORER_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{},
                    WriteProcessor.WriteProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{});

    public static final TaskPlan<CreateSegmentRequest, CreateSegmentResponse> CREATE_SEGMENT_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{},
                    CreateSegmentProcessor.CreateSegmentProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{});
    public static final TaskPlan<GetMessageFromStorerRequest, GetMessageFromStorerResponse>
            GET_MESSAGE_FROM_STORER_TASK_PLAN = new TaskPlan<>(new Preparer[]{},
            ReadProcessor.ReadProcessorEnum.INSTANCE.getInstance(),
            new Finisher[]{});
}
