package org.catmq.context;

import io.grpc.*;
import io.grpc.Context;

public class ContextInterceptor implements ServerInterceptor {
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
            ServerCall<R, W> call,
            Metadata headers,
            ServerCallHandler<R, W> next
    ) {
        io.grpc.Context context = Context.current().withValue(Context.key("rpc-metadata"), headers);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
