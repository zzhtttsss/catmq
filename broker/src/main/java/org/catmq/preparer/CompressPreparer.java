package org.catmq.preparer;

import org.catmq.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressPreparer implements Preparer {
    Logger logger = LoggerFactory.getLogger(CompressPreparer.class);
    public static final String COMPRESS_PREPARER = "CompressPreparer";

    public RequestContext prepare(RequestContext ctx) {
        logger.info("CompressPreparer");

        return ctx;
    }
}
