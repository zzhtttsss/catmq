package org.catmq.preparer;

import org.catmq.context.Context;

public class AuthPreparer implements Preparer {

    public static final String AUTH_PREPARER = "AuthPreparer";

    @Override
    public Context prepare(Context ctx) {
        ctx.testField = ctx.testField + " AuthPreparer";
        return ctx;
    }
}
