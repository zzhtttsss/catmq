package org.catmq.preparer;

import org.catmq.context.RequestContext;

public class StorerPreparer implements Preparer {
    @Override
    public RequestContext prepare(RequestContext ctx) {
        return ctx;
    }
}
