package org.catmq.finisher;

import org.catmq.context.RequestContext;

public class ExampleFinisher implements Finisher {

    public static final String EXAMPLE_FINISHER = "ExampleFinisher";

    @Override
    public RequestContext finish(RequestContext ctx) {
        ctx.testField = ctx.testField + " ExampleFinisher";
        return ctx;
    }
}
