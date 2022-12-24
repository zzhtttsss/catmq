package org.catmq.finisher;

import org.catmq.context.RequestContext;

public interface Finisher {
    RequestContext finish(RequestContext ctx);
}
