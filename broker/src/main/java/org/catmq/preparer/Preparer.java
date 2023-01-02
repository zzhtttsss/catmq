package org.catmq.preparer;

import org.catmq.grpc.RequestContext;

public interface Preparer {
    void prepare(RequestContext ctx);

}
