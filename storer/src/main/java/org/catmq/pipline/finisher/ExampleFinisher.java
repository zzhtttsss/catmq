package org.catmq.pipline.finisher;

import lombok.extern.slf4j.Slf4j;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Finisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ExampleFinisher implements Finisher {
    public static final String EXAMPLE_FINISHER = "ExampleFinisher";

    @Override
    public void finish(RequestContext ctx) {
        log.debug("ExampleFinisher");
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
