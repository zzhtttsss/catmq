package org.catmq.storer;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.catmq.pipline.TaskPlan;
import org.catmq.grpc.InterceptorConstants;
import org.catmq.grpc.RequestContext;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.grpc.ResponseWriter;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.*;
import org.catmq.thread.OrderedExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.catmq.storer.StorerConfig.*;
import static org.catmq.thread.OrderedExecutor.NO_TASK_LIMIT;
import static org.catmq.util.StringUtil.defaultString;

@Slf4j
public class StorerServer extends StorerServiceGrpc.StorerServiceImplBase {
    protected OrderedExecutor writeOrderedExecutor;

    protected OrderedExecutor readThreadPoolExecutor;


    public StorerServer() {
        StorerConfig config = StorerConfig.StorerConfigEnum.INSTANCE.getInstance();
        writeOrderedExecutor = createExecutor(config.getWriteOrderedExecutorThreadNums(), WRITE_ORDERED_EXECUTOR_NAME,
                NO_TASK_LIMIT);
        readThreadPoolExecutor = createExecutor(config.getReadOrderedExecutorThreadNums(), READ_ORDERED_EXECUTOR_NAME,
                NO_TASK_LIMIT);


        this.init();
    }

    protected void init() {

    }

    private OrderedExecutor createExecutor(
            int numThreads,
            String nameFormat,
            int maxTasksInQueue) {
        if (numThreads <= 0) {
            return null;
        } else {
            return OrderedExecutor.newBuilder()
                    .numThreads(numThreads)
                    .name(nameFormat)
                    .traceTaskExecution(false)
                    .preserveMdcForTaskExecution(false)
                    .maxTasksInQueue(maxTasksInQueue)
                    .enableThreadScopedMetrics(true)
                    .build();
        }
    }

    @Override
    public void sendMessage2Storer(SendMessage2StorerRequest request, StreamObserver<SendMessage2StorerResponse> responseObserver) {
        Function<Status, SendMessage2StorerResponse> statusResponseCreator = status -> SendMessage2StorerResponse.newBuilder().setStatus(status).build();
        RequestContext ctx = createContext();
        try {
            this.writeOrderedExecutor.submit(new GrpcTask<>(ctx, request, TaskPlan.SEND_MESSAGE_2_STORER_TASK_PLAN,
                    responseObserver, statusResponseCreator));
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    protected <V, T> void writeResponse(RequestContext context, V request, T response, StreamObserver<T> responseObserver,
                                        Throwable t, Function<Status, T> errorResponseCreator) {
        if (t != null) {
            ResponseWriter.ResponseWriterEnum.INSTANCE.getInstance().write(
                    responseObserver,
                    errorResponseCreator.apply(convertExceptionToStatus(t))
            );
        } else {
            ResponseWriter.ResponseWriterEnum.INSTANCE.getInstance().write(responseObserver, response);
        }
    }

    protected Status convertExceptionToStatus(Throwable t) {
        return ResponseBuilder.ResponseBuilderEnum.INSTANCE.getInstance().buildStatus(t);
    }

    protected Status flowLimitStatus() {
        return ResponseBuilder.ResponseBuilderEnum.INSTANCE.getInstance().buildStatus(Code.TOO_MANY_REQUESTS, "flow limit");
    }

    protected RequestContext createContext() {
        Context ctx = Context.current();
        Metadata headers = InterceptorConstants.METADATA.get(ctx);
        RequestContext context = RequestContext.create()
                .setLocalAddress(getValueFromMetadata(headers, InterceptorConstants.LOCAL_ADDRESS))
                .setRemoteAddress(getValueFromMetadata(headers, InterceptorConstants.REMOTE_ADDRESS))
                .setClientID(getValueFromMetadata(headers, InterceptorConstants.CLIENT_ID))
                .setLanguage(getValueFromMetadata(headers, InterceptorConstants.LANGUAGE))
                .setClientVersion(getValueFromMetadata(headers, InterceptorConstants.CLIENT_VERSION))
                .setAction(getValueFromMetadata(headers, InterceptorConstants.RPC_NAME));
        return context;
    }

    protected String getValueFromMetadata(Metadata headers, Metadata.Key<String> key) {
        return defaultString(headers.get(key));
    }

    protected class GrpcTask<V, T> implements Runnable {

        protected final RequestContext ctx;
        protected final V request;

        protected final TaskPlan<V, T> taskPlan;

        protected final Function<Status, T> statusResponseCreator;
        protected final StreamObserver<T> streamObserver;

        public GrpcTask(RequestContext ctx, V request, TaskPlan<V, T> taskPlan, StreamObserver<T> streamObserver,
                        Function<Status, T> statusResponseCreator) {
            this.ctx = ctx;
            this.taskPlan = taskPlan;
            this.streamObserver = streamObserver;
            this.request = request;
            this.statusResponseCreator = statusResponseCreator;

        }

        public CompletableFuture<T> execute(RequestContext ctx, V request, TaskPlan<V, T> taskPlan){
            CompletableFuture<T> future = new CompletableFuture<>();
            return future;
        }

        @Override
        public void run() {
            execute(ctx, request, taskPlan)
                    .whenComplete((response, throwable) -> writeResponse(ctx, request, response, streamObserver, throwable, statusResponseCreator));
        }
    }

}
