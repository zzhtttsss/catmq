package org.catmq.preparer;

import org.catmq.context.RequestContext;

public class CompressPreparer implements Preparer {

    public static final String COMPRESS_PREPARER = "CompressPreparer";

    public RequestContext prepare(RequestContext ctx) {
        ctx.testField = ctx.testField + " CompressPreparer";

        return ctx;
    }
}
