package org.catmq.processor;

import org.catmq.context.RequestContext;
import org.catmq.grpc.ResponseBuilder;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

public class ProduceProcessor implements Processor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";

    @Override
    public SendMessage2BrokerResponse process(RequestContext ctx, SendMessage2BrokerRequest request) {
        return SendMessage2BrokerResponse.newBuilder().setAck(true).setRes("oh!").build();
    }

    public enum ProduceProcessorEnum {
        INSTANCE;
        private final ProduceProcessor produceProcessor;
        ProduceProcessorEnum() {
            produceProcessor = new ProduceProcessor();
        }
        public ProduceProcessor getInstance() {
            return produceProcessor;
        }
    }
}