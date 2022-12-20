package org.catmq.processor;

import org.catmq.context.Context;

public interface Processor {

    Context process(Context ctx);
}
