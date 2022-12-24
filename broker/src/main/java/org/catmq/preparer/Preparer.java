package org.catmq.preparer;

import org.catmq.context.RequestContext;

public interface Preparer {
    RequestContext prepare(RequestContext ctx);

}
