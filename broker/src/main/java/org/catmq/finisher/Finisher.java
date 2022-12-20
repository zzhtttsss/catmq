package org.catmq.finisher;

import org.catmq.context.Context;
import org.checkerframework.checker.units.qual.C;

public interface Finisher {
    Context finish(Context ctx);
}
