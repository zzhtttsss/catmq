package org.catmq.pipline;

import org.catmq.grpc.RequestContext;

public interface Preparer {
    void prepare(RequestContext ctx);

}
