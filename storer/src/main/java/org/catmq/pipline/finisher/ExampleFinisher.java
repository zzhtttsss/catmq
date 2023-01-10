package org.catmq.pipline.finisher;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Finisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleFinisher implements Finisher {
    Logger logger = LoggerFactory.getLogger(ExampleFinisher.class);
    public static final String EXAMPLE_FINISHER = "ExampleFinisher";

    @Override
    public void finish(RequestContext ctx) {
        logger.info("ExampleFinisher");
    }

    public enum ExampleFinisherEnum {
        INSTANCE;
        private final ExampleFinisher exampleFinisher;
        ExampleFinisherEnum() {
            exampleFinisher = new ExampleFinisher();
        }
        public ExampleFinisher getInstance() {
            return exampleFinisher;
        }
    }
}