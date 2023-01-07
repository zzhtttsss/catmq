package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

@Slf4j
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
