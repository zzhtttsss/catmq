package org.catmq.processor;

import org.catmq.context.RequestContext;

public interface Processor<V, T> {

    T process(RequestContext ctx, V request);
}
