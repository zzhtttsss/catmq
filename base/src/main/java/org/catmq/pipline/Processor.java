package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

/**
 * Represent how the grpc request should be processed.
 */
public interface Processor<V, T> {

    T process(RequestContext ctx, V request);
}
