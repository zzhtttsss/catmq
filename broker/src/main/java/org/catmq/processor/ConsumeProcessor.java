package org.catmq.processor;

import org.catmq.context.Context;

public class ConsumeProcessor implements Processor {

    public static final String CONSUME_PROCESSOR = "ConsumeProcessor";

    @Override
    public Context process(Context ctx) {
        ctx.testField = ctx.testField + " ConsumeProcessor";

        return ctx;
    }
}
