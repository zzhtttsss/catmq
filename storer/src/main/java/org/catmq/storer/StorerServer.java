package org.catmq.storer;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.catmq.entity.TopicMode;
import org.catmq.grpc.InterceptorConstants;
import org.catmq.grpc.RequestContext;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.grpc.ResponseWriter;
import org.catmq.pipline.Finisher;
import org.catmq.pipline.Preparer;
import org.catmq.pipline.TaskPlan;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.*;
import org.catmq.thread.OrderedExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.catmq.entity.StorerConfig.*;
import static org.catmq.thread.OrderedExecutor.NO_TASK_LIMIT;
import static org.catmq.util.StringUtil.defaultString;

/**
 * Handle all grpc request.
 */
@Slf4j
public class StorerServer extends StorerServiceGrpc.StorerServiceImplBase {
    /**
     * Handle all write request.
     */
    protected OrderedExecutor writeOrderedExecutor;
    /**
     * Handle all read request.
     */
    protected OrderedExecutor readThreadPoolExecutor;


    public StorerServer() {
        writeOrderedExecutor = createExecutor(STORER_CONFIG.getWriteOrderedExecutorThreadNums(),
                WRITE_ORDERED_EXECUTOR_NAME, NO_TASK_LIMIT);
        readThreadPoolExecutor = createExecutor(STORER_CONFIG.getReadOrderedExecutorThreadNums(),
                READ_ORDERED_EXECUTOR_NAME, NO_TASK_LIMIT);

        this.init();
    }

    protected void init() {
    }

    private OrderedExecutor createExecutor(int numThreads, String nameFormat, int maxTasksInQueue) {
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
        Function<Status, SendMessage2StorerResponse> statusResponseCreator =
                status -> SendMessage2StorerResponse.newBuilder().setStatus(status).build();
//        log.info("receive a batch of message, num: {}", request.getMessageCount());
        RequestContext ctx = createContext();

        try {
            switch (TopicMode.fromString(request.getMode())) {
                case NORMAL -> this.writeOrderedExecutor.execute(new GrpcTask<>(ctx, request,
                        TaskPlan.SEND_MESSAGE_2_STORER_TASK_PLAN, responseObserver, statusResponseCreator));
                case ORDERED -> this.writeOrderedExecutor.executeOrdered(request.getMessage(0).getSegmentId(),
                        new GrpcTask<>(ctx, request, TaskPlan.SEND_MESSAGE_2_STORER_TASK_PLAN, responseObserver, statusResponseCreator));
                default -> throw new RuntimeException("Unsupported topic mode.");
            }

        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void createSegment(CreateSegmentRequest request, StreamObserver<CreateSegmentResponse> responseObserver) {
        Function<Status, CreateSegmentResponse> statusResponseCreator =
                status -> CreateSegmentResponse.newBuilder().setStatus(status).build();
        log.debug("receive a request to create a segment, id: {}", request.getSegmentId());
        RequestContext ctx = createContext();

        try {
            this.writeOrderedExecutor.executeOrdered(ctx.getSegmentId(), new GrpcTask<>(ctx, request,
                    TaskPlan.CREATE_SEGMENT_TASK_PLAN, responseObserver, statusResponseCreator));
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void getMessageFromStorer(GetMessageFromStorerRequest request, StreamObserver<GetMessageFromStorerResponse> responseObserver) {
        Function<Status, GetMessageFromStorerResponse> statusResponseCreator =
                status -> GetMessageFromStorerResponse.newBuilder().setStatus(status).build();
        log.debug("receive a request to get message from storer, segmentId: {}", request.getSegmentId());
        RequestContext ctx = createContext();

        try {
            this.readThreadPoolExecutor.executeOrdered(request.getSegmentId(), new GrpcTask<>(ctx, request,
                    TaskPlan.GET_MESSAGE_FROM_STORER_TASK_PLAN, responseObserver, statusResponseCreator));
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    /**
     * Handle and write response to the {@link StreamObserver}.
     *
     * @param context              the context of the request
     * @param request              the grpc request
     * @param response             the grpc response
     * @param responseObserver     {@link StreamObserver} to handle the response
     * @param t                    exception
     * @param errorResponseCreator {@link Function} to handle error
     * @param <V>                  class of grpc request
     * @param <T>                  class of grpc response
     */
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

    /**
     * Get the metadata of the grpc request and create a {@link RequestContext} with it.
     *
     * @return the context of the grpc request
     */
    protected RequestContext createContext() {
        Context ctx = Context.current();
        Metadata headers = InterceptorConstants.METADATA.get(ctx);
        RequestContext context = RequestContext.create()
                .setLocalAddress(getValueFromMetadata(headers, InterceptorConstants.LOCAL_ADDRESS))
                .setRemoteAddress(getValueFromMetadata(headers, InterceptorConstants.REMOTE_ADDRESS))
                .setAction(getValueFromMetadata(headers, InterceptorConstants.RPC_NAME));
        return context;
    }

    protected String getValueFromMetadata(Metadata headers, Metadata.Key<String> key) {
        return defaultString(headers.get(key));
    }

    /**
     * Represent a grpc request that need to be handled.
     *
     * @param <V> class of grpc request
     * @param <T> class of grpc response
     */
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

        /**
         * Process the grpc request like a pipeline.
         *
         * @param ctx      the context of the grpc request
         * @param request  the grpc request
         * @param taskPlan the plan of the pipeline
         * @return {@link CompletableFuture} with response to be handled
         */
        public CompletableFuture<T> execute(RequestContext ctx, V request, TaskPlan<V, T> taskPlan) {
            CompletableFuture<T> future = new CompletableFuture<>();
            try {
                for (Preparer p : taskPlan.preparers()) {
                    p.prepare(ctx);
                }
                T response = taskPlan.processor().process(ctx, request);
                for (Finisher f : taskPlan.finishers()) {
                    f.finish(ctx);
                }
                future.complete(response);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
            return future;
        }

        /**
         * Process the grpc request and handle the response.
         */
        @Override
        public void run() {
            execute(ctx, request, taskPlan)
                    .whenComplete((response, throwable) -> writeResponse(ctx, request, response, streamObserver,
                            throwable, statusResponseCreator));
        }
    }

}
