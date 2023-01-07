package org.catmq.pipline.preparer;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Preparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplePreparer implements Preparer {
    Logger logger = LoggerFactory.getLogger(ExamplePreparer.class);
    public static final String EXAMPLE_PREPARER = "ExamplePreparer";

    public void prepare(RequestContext ctx) {
        logger.info("ExamplePreparer");

    }

    public enum ExamplePreparerEnum {
        INSTANCE;
        private final ExamplePreparer examplePreparer;
        ExamplePreparerEnum() {
            examplePreparer = new ExamplePreparer();
        }
        public ExamplePreparer getInstance() {
            return examplePreparer;
        }
    }
}
