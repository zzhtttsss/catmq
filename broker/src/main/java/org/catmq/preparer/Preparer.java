package org.catmq.preparer;

import org.catmq.context.RequestContext;

public interface Preparer {
    void prepare(RequestContext ctx);

}
