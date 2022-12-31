package org.catmq.finisher;

import org.catmq.context.RequestContext;

public interface Finisher {
    void finish(RequestContext ctx);
}
