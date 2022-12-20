package org.catmq.preparer;

import org.catmq.context.Context;

public class CompressPreparer implements Preparer {

    public static final String COMPRESS_PREPARER = "CompressPreparer";

    public Context prepare(Context ctx) {
        ctx.testField = ctx.testField + " CompressPreparer";

        return ctx;
    }
}
