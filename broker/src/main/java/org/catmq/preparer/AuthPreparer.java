package org.catmq.preparer;

import org.catmq.context.RequestContext;

public class AuthPreparer implements Preparer {

    public static final String AUTH_PREPARER = "AuthPreparer";

    @Override
    public RequestContext prepare(RequestContext ctx) {
        ctx.testField = ctx.testField + " AuthPreparer";
        return ctx;
    }
}
