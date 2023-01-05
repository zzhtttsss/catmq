package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

public interface Finisher {
    void finish(RequestContext ctx);
}
