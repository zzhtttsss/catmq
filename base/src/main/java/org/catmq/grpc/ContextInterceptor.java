package org.catmq.grpc;

import io.grpc.*;
import io.grpc.Context;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContextInterceptor implements ServerInterceptor {

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
            ServerCall<R, W> call,
            Metadata headers,
            ServerCallHandler<R, W> next
    ) {
        log.info(headers.toString());
        io.grpc.Context context = Context.current().withValue(InterceptorConstants.METADATA, headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
