package org.catmq;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import org.catmq.context.InterceptorConstants;
import org.catmq.context.RequestContext;
import org.catmq.executor.Executor;
import org.catmq.executor.TaskPlan;
import org.catmq.finisher.ExampleFinisher;
import org.catmq.finisher.Finisher;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.grpc.ResponseWriter;
import org.catmq.preparer.AuthPreparer;
import org.catmq.preparer.CompressPreparer;
import org.catmq.preparer.Preparer;
import org.catmq.processor.ProduceProcessor;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.thread.ThreadPoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.catmq.util.StringUtil.defaultString;

public class BrokerServer extends BrokerServiceGrpc.BrokerServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(BrokerStartup.class.getName());

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
    }

    @Override
    public void sendMessage2Broker(SendMessage2BrokerRequest request, StreamObserver<SendMessage2BrokerResponse> responseObserver) {

        Function<Status, SendMessage2BrokerResponse> statusResponseCreator = status -> SendMessage2BrokerResponse.newBuilder().setStatus(status).build();
        RequestContext ctx = createContext();
        TaskPlan<SendMessage2BrokerRequest, SendMessage2BrokerResponse> taskPlan =
                new TaskPlan<>(new Preparer[]{new AuthPreparer(), new CompressPreparer()}, new ProduceProcessor(), new Finisher[]{new ExampleFinisher()});
        Executor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> executor = new Executor<>();
        try {
            this.submit2Executor(this.producerThreadPoolExecutor,
                    ctx,
                    request,
                    () -> executor.execute(ctx, request, taskPlan)
                            .whenComplete((response, throwable) -> writeResponse(ctx, request, response, responseObserver, throwable, statusResponseCreator)),
                    responseObserver,
                    statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(ctx, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    protected <V, T> void submit2Executor(ExecutorService executor, RequestContext context, V request, Runnable runnable,
                                          StreamObserver<T> responseObserver, Function<Status, T> statusResponseCreator) {
        executor.submit(new GrpcTask<>(runnable, context, request, responseObserver, statusResponseCreator.apply(flowLimitStatus())));
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

    protected static class GrpcTask<V, T> implements Runnable {

        protected final Runnable runnable;
        protected final RequestContext context;
        protected final V request;

        protected final T executeRejectResponse;
        protected final StreamObserver<T> streamObserver;

        public GrpcTask(Runnable runnable, RequestContext context, V request, StreamObserver<T> streamObserver,
                        T executeRejectResponse) {
            this.runnable = runnable;
            this.context = context;
            this.streamObserver = streamObserver;
            this.request = request;
            this.executeRejectResponse = executeRejectResponse;

        }

        @Override
        public void run() {
            this.runnable.run();
        }
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

}
