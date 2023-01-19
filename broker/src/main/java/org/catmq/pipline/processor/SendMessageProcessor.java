package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.Topic;
import org.catmq.broker.topic.TopicName;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

@Slf4j
public class SendMessageProcessor implements Processor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";
    private final TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();

    @Override
    public SendMessage2BrokerResponse process(RequestContext ctx, SendMessage2BrokerRequest request) {
        TopicName topicName = TopicName.get(request.getTopic());
        Topic topic = topicService.getTopic(topicName.getCompleteTopicName());
        topic.createSubscription(topicName.getCompleteTopicName());
        topic.putMessage(request.getMessage());
        return SendMessage2BrokerResponse
                .newBuilder()
                .setAck(true)
                .setRes("send success")
                .build();
    }

    public enum ProduceProcessorEnum {
        INSTANCE;
        private final SendMessageProcessor sendMessageProcessor;

        ProduceProcessorEnum() {
            sendMessageProcessor = new SendMessageProcessor();
        }

        public SendMessageProcessor getInstance() {
            return sendMessageProcessor;
        }
    }
}
