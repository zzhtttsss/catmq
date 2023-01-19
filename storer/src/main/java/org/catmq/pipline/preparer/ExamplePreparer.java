package org.catmq.pipline.preparer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Preparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ExamplePreparer implements Preparer {
    public static final String EXAMPLE_PREPARER = "ExamplePreparer";

    public void prepare(RequestContext ctx) {
        log.debug("ExamplePreparer");

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
