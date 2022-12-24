package org.catmq.context;

import io.grpc.*;
import io.grpc.Context;
import org.catmq.thread.ThreadPoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextInterceptor implements ServerInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(ContextInterceptor.class);

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
            ServerCall<R, W> call,
            Metadata headers,
            ServerCallHandler<R, W> next
    ) {
        logger.info(headers.toString());
        io.grpc.Context context = Context.current().withValue(InterceptorConstants.METADATA, headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
