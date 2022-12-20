package org.catmq.finisher;

import org.catmq.context.Context;

public class ExampleFinisher implements Finisher {

    public static final String EXAMPLE_FINISHER = "ExampleFinisher";

    @Override
    public Context finish(Context ctx) {
        ctx.testField = ctx.testField + " ExampleFinisher";
        return ctx;
    }
}
