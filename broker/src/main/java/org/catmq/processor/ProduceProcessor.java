package org.catmq.processor;

import org.catmq.context.Context;
import org.catmq.executer.Executer;

public class ProduceProcessor implements Processor {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";

    @Override
    public Context process(Context ctx) {
        ctx.testField = ctx.testField + " ProduceProcessor";
        return ctx;
    }
}
