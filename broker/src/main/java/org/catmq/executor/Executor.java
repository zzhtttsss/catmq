package org.catmq.executor;

import org.catmq.context.RequestContext;
import org.catmq.finisher.ExampleFinisher;
import org.catmq.finisher.Finisher;
import org.catmq.preparer.AuthPreparer;
import org.catmq.preparer.CompressPreparer;
import org.catmq.preparer.Preparer;
import org.catmq.processor.Processor;
import org.catmq.processor.ProduceProcessor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.catmq.finisher.ExampleFinisher.EXAMPLE_FINISHER;
import static org.catmq.preparer.AuthPreparer.AUTH_PREPARER;
import static org.catmq.preparer.CompressPreparer.COMPRESS_PREPARER;
import static org.catmq.processor.ProduceProcessor.PRODUCE_PROCESSOR_NAME;

public class Executor<V, T> {
    //    public static GrpcTask getExecuterByContext(RequestContext ctx) {
//        String config = "{\"preparers\":[\"AuthPreparer\",\"CompressPreparer\"],\"processor\":" +
//                "\"ProduceProcessor\",\"finishers\":[\"ExampleFinisher\"]}";
//        return JSON.parseObject(config, GrpcTask.class);
//    }

    public CompletableFuture<T> execute(RequestContext ctx, V request, TaskPlan<V, T> taskPlan){
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            for (Preparer p: taskPlan.getPreparers()) {
                p.prepare(ctx);
            }
            T response = taskPlan.getProcessor().process(ctx, request);
            for (Finisher f: taskPlan.getFinishers()) {
                f.finish(ctx);
            }
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
