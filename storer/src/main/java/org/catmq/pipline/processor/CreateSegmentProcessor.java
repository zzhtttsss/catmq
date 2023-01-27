package org.catmq.pipline.processor;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.CreateSegmentRequest;
import org.catmq.protocol.service.CreateSegmentResponse;
import org.catmq.storer.Storer;

public class CreateSegmentProcessor implements Processor<CreateSegmentRequest, CreateSegmentResponse> {
    public enum CreateSegmentProcessorEnum {
        INSTANCE;
        private final CreateSegmentProcessor createSegmentProcessor;

        CreateSegmentProcessorEnum() {
            createSegmentProcessor = new CreateSegmentProcessor();
        }

        public CreateSegmentProcessor getInstance() {
            return createSegmentProcessor;
        }
    }

    @Override
    public CreateSegmentResponse process(RequestContext ctx, CreateSegmentRequest request) {
        Storer storer = Storer.STORER;


        CreateSegmentResponse response = CreateSegmentResponse.newBuilder().setAck(true).setRes("Success").build();
        return response;
    }
}
