package org.catmq.preparer;

import org.catmq.context.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthPreparer implements Preparer {

    Logger logger = LoggerFactory.getLogger(AuthPreparer.class);

    public static final String AUTH_PREPARER = "AuthPreparer";

    @Override
    public RequestContext prepare(RequestContext ctx) {
        logger.info("AuthPreparer");
        return ctx;
    }
}
