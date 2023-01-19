package org.catmq.broker;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.catmq.context.InterceptorConstants;
import org.catmq.context.RequestContext;
import org.catmq.context.TaskPlan;
import org.catmq.finisher.Finisher;
import org.catmq.pipline.TaskPlan;
import org.catmq.grpc.InterceptorConstants;
import org.catmq.grpc.RequestContext;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.grpc.ResponseWriter;
import org.catmq.pipline.Finisher;
import org.catmq.pipline.Preparer;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.*;
import org.catmq.thread.ThreadFactoryWithIndex;
import org.catmq.thread.ThreadPoolMonitor;
import org.catmq.zk.BrokerZooKeeper;

import java.util.concurrent.*;
import java.util.function.Function;

import static org.catmq.util.StringUtil.defaultString;

@Slf4j
public class BrokerServer extends BrokerServiceGrpc.BrokerServiceImplBase {

    public BrokerInfo brokerInfo;
    public BrokerZooKeeper brokerZooKeeper;

    private ScheduledExecutorService timeExecutor;

    protected ThreadPoolExecutor producerThreadPoolExecutor;

    protected void init() {
        GrpcTaskRejectedExecutionHandler rejectedExecutionHandler = new GrpcTaskRejectedExecutionHandler();
        this.producerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);

        this.brokerZooKeeper.register2Zk();
        log.info("BrokerServer init success");

    }

    public void close() {
        this.brokerZooKeeper.close();
    }

    @Override
    public void sendMessage2Broker(SendMessage2BrokerRequest request, StreamObserver<SendMessage2BrokerResponse> responseObserver) {
        Function<Status, SendMessage2BrokerResponse> statusResponseCreator = status -> SendMessage2BrokerResponse
                .newBuilder()
                .setStatus(status)
                .build();
        RequestContext ctx = createContext();
        try {
            this.producerThreadPoolExecutor.submit(new GrpcTask<>(ctx, request,
                    TaskPlan.SEND_MESSAGE_2_BROKER_TASK_PLAN, responseObserver, statusResponseCreator));
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void createTopic(CreateTopicRequest request, StreamObserver<CreateTopicResponse> responseObserver) {
        Function<Status, CreateTopicResponse> statusResponseCreator = status -> CreateTopicResponse
                .newBuilder()
                .setStatus(status)
                .build();
        RequestContext ctx = createContext().setBrokerPath(this.brokerZooKeeper.getBrokerPath());
        try {
            this.producerThreadPoolExecutor.submit(new GrpcTask<>(ctx, request,
                    TaskPlan.CREATE_TOPIC_TASK_PLAN, responseObserver, statusResponseCreator));
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void getMessageFromBroker(GetMessageFromBrokerRequest request, StreamObserver<GetMessageFromBrokerResponse> responseObserver) {
        Function<Status, GetMessageFromBrokerResponse> statusResponseCreator = status -> GetMessageFromBrokerResponse
                .newBuilder()
                .setStatus(status)
                .build();
        RequestContext ctx = createContext().setConsumerId(request.getConsumerId());
        try {
            this.producerThreadPoolExecutor.submit(new GrpcTask<>(ctx, request,
                    TaskPlan.GET_MESSAGE_FROM_BROKER_TASK_PLAN, responseObserver, statusResponseCreator));
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
        return RequestContext.create()
                .setLocalAddress(getValueFromMetadata(headers, InterceptorConstants.LOCAL_ADDRESS))
                .setRemoteAddress(getValueFromMetadata(headers, InterceptorConstants.REMOTE_ADDRESS))
                .setAction(getValueFromMetadata(headers, InterceptorConstants.RPC_NAME));
    }

    protected String getValueFromMetadata(Metadata headers, Metadata.Key<String> key) {
        return defaultString(headers.get(key));
    }

    public BrokerServer() {
        BrokerConfig config = BrokerConfig.BrokerConfigEnum.INSTANCE.getInstance();
        this.producerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
                config.getGrpcProducerThreadPoolNums(),
                config.getGrpcProducerThreadPoolNums(),
                1,
                TimeUnit.MINUTES,
                "GrpcProducerThreadPool",
                config.getGrpcProducerThreadQueueCapacity()
        );
        this.brokerInfo = new BrokerInfo(config);
        this.brokerZooKeeper = new BrokerZooKeeper(config.getZkAddress(), this);
        this.timeExecutor = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryWithIndex("BrokerTimerThread_"));
        this.init();
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

        @Override
        public void run() {
            execute(ctx, request, taskPlan)
                    .whenComplete((response, throwable) -> writeResponse(ctx, request, response, streamObserver, throwable, statusResponseCreator));
        }
    }

    protected class GrpcTaskRejectedExecutionHandler implements RejectedExecutionHandler {

        public GrpcTaskRejectedExecutionHandler() {

        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof GrpcTask) {
                try {
                    GrpcTask grpcTask = (GrpcTask) r;
                    writeResponse(grpcTask.ctx, grpcTask.request, grpcTask.statusResponseCreator.apply(flowLimitStatus()), grpcTask.streamObserver, null, null);
                } catch (Throwable t) {
                    log.warn("write rejected error response failed", t);
                }
            }
        }
    }

}
