package org.catmq.pipline;

import org.catmq.pipline.finisher.ExampleFinisher;
import org.catmq.pipline.preparer.ExamplePreparer;
import org.catmq.pipline.processor.WriteProcessor;
import org.catmq.protocol.service.SendMessage2StorerRequest;
import org.catmq.protocol.service.SendMessage2StorerResponse;

/**
 * Represent how to process a grpc request.
 *
 * @param preparers all {@link Preparer} need to be done
 * @param processor processor that handle main logic
 * @param finishers all {@link Finisher} need to be done
 * @param <V> class of the grpc request
 * @param <T> class of the grpc response
 */
public record TaskPlan<V, T>(Preparer[] preparers, Processor<V, T> processor, Finisher[] finishers) {

    public static final TaskPlan<SendMessage2StorerRequest, SendMessage2StorerResponse> SEND_MESSAGE_2_STORER_TASK_PLAN =
            new TaskPlan<>(new Preparer[]{ExamplePreparer.ExamplePreparerEnum.INSTANCE.getInstance()},
                    WriteProcessor.WriteProcessorEnum.INSTANCE.getInstance(),
                    new Finisher[]{ExampleFinisher.ExampleFinisherEnum.INSTANCE.getInstance()});
}
