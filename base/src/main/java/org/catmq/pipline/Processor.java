package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

public interface Processor<V, T> {

    T process(RequestContext ctx, V request);
}
