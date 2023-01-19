package org.catmq.processor;

import org.catmq.broker.common.Consumer;
import org.catmq.broker.service.ClientManageService;
import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.ITopic;
import org.catmq.broker.topic.TopicName;
import org.catmq.context.RequestContext;
import org.catmq.protocol.service.GetMessageFromBrokerRequest;
import org.catmq.protocol.service.GetMessageFromBrokerResponse;

import java.util.Optional;

public class GetMessageProcessor implements Processor<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse> {

    public static final String CONSUME_PROCESSOR_NAME = "ConsumeProcessor";

    private final TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();
    private final ClientManageService clientManageService = ClientManageService.ClientManageServiceEnum.INSTANCE.getInstance();

    @Override
    public GetMessageFromBrokerResponse process(RequestContext ctx, GetMessageFromBrokerRequest request) {
        TopicName topicName = TopicName.get(request.getTopic());
        ITopic topic = topicService.getTopic(topicName.getCompleteTopicName());
        if (!topic.isSubscribe(topicName.getCompleteTopicName(), request.getConsumerId())) {
            topic.subscribe(topicName.getCompleteTopicName(), request.getConsumerId());
        }
        Consumer consumer = clientManageService.getConsumer(request.getConsumerId());
        Optional<String> message = consumer.getMessage();
        return message
                .map(s -> GetMessageFromBrokerResponse
                        .newBuilder()
                        .setAck(true)
                        .setRes(s)
                        .build())
                .orElseGet(() -> GetMessageFromBrokerResponse
                        .newBuilder()
                        .setAck(false)
                        .setRes("no message yet")
                        .build());

    }


    public enum GetMessageProcessorEnum {
        INSTANCE;
        private final GetMessageProcessor getMessageProcessor;

        GetMessageProcessorEnum() {
            getMessageProcessor = new GetMessageProcessor();
        }

        public GetMessageProcessor getInstance() {
            return getMessageProcessor;
        }
    }
}
