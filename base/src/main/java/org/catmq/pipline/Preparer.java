package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

/**
 * Represent what should be done before the grpc request being processed.
 */
public interface Preparer {
    void prepare(RequestContext ctx);

}
