package org.catmq.finisher;

import org.catmq.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleFinisher implements Finisher {
    Logger logger = LoggerFactory.getLogger(ExampleFinisher.class);
    public static final String EXAMPLE_FINISHER = "ExampleFinisher";

    @Override
    public RequestContext finish(RequestContext ctx) {
        logger.info("ExampleFinisher");
        return ctx;
    }
}
