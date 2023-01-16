package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

/**
 * Represent what should be done after the grpc request being processed.
 */
public interface Finisher {
    void finish(RequestContext ctx);
}
