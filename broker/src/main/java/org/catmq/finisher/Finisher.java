package org.catmq.finisher;

import org.catmq.grpc.RequestContext;

public interface Finisher {
    void finish(RequestContext ctx);
}
