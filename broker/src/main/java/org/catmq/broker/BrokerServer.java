package org.catmq.broker;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.catmq.context.InterceptorConstants;
import org.catmq.context.RequestContext;
import org.catmq.context.TaskPlan;
import org.catmq.finisher.Finisher;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.grpc.ResponseWriter;
import org.catmq.preparer.Preparer;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.thread.ThreadFactoryWithIndex;
import org.catmq.thread.ThreadPoolMonitor;
import org.catmq.zk.BrokerZooKeeper;

import java.util.concurrent.*;
import java.util.function.Function;

import static org.catmq.util.StringUtil.defaultString;

@Slf4j
public class BrokerServer extends BrokerServiceGrpc.BrokerServiceImplBase {

    public BrokerInfo brokerInfo;
    public BrokerZooKeeper bzk;
    private ScheduledExecutorService timeExecutor;

    protected ThreadPoolExecutor producerThreadPoolExecutor;

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
        this.brokerInfo = new BrokerInfo(config.getBrokerId(), config.getBrokerName(),
                config.getBrokerIp(), config.getBrokerPort(), "127.0.0.1:2181");
        this.bzk = new BrokerZooKeeper("127.0.0.1:2181", this);
        this.timeExecutor = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryWithIndex("BrokerTimerThread_"));
        this.init();
    }

    protected void init() {
        GrpcTaskRejectedExecutionHandler rejectedExecutionHandler = new GrpcTaskRejectedExecutionHandler();
        this.producerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);

        this.bzk.register2Zk();
        log.info("BrokerServer init success");

    }

    @Override
    public void sendMessage2Broker(SendMessage2BrokerRequest request, StreamObserver<SendMessage2BrokerResponse> responseObserver) {
        Function<Status, SendMessage2BrokerResponse> statusResponseCreator = status -> SendMessage2BrokerResponse.newBuilder().setStatus(status).build();
        RequestContext ctx = createContext();
        try {
            this.producerThreadPoolExecutor.submit(new GrpcTask<>(ctx, request, TaskPlan.SEND_MESSAGE_2_BROKER_TASK_PLAN, responseObserver, statusResponseCreator));
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
