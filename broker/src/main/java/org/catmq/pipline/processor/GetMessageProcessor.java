package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.manager.ClientManager;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.ConsumerBatchPolicy;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.ContextVariable;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.service.GetMessageFromBrokerRequest;
import org.catmq.protocol.service.GetMessageFromBrokerResponse;

import java.util.List;

@Slf4j
public class GetMessageProcessor implements Processor<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse> {

    public static final String CONSUME_PROCESSOR_NAME = "ConsumeProcessor";

    private final TopicManager topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();
    private final ClientManager clientManager = ClientManager.ClientManagerEnum.INSTANCE.getInstance();

    @Override
    public GetMessageFromBrokerResponse process(RequestContext ctx, GetMessageFromBrokerRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        Topic topic = topicManager.getTopic(topicDetail.getCompleteTopicName());
        if (!topic.isSubscribe(request.getSubscription(), request.getConsumerId())) {
            topic.subscribe(request.getSubscription(), request.getConsumerId());
        }
        Consumer consumer = clientManager.getConsumer(request.getConsumerId());
        ConsumerBatchPolicy policy = ctx.getVal(ContextVariable.CONSUME_BATCH_POLICY, ConsumerBatchPolicy.class);
        List<NumberedMessage> message = consumer.getBatchMessage(policy);
        log.warn("get message from broker: {}", message);
        return GetMessageFromBrokerResponse
                .newBuilder()
                .addAllMessage(message)
                .build();

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
