package org.catmq.pipline.processor;

import org.catmq.broker.Broker;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.CreateTopicRequest;
import org.catmq.protocol.service.CreateTopicResponse;

public class CreateTopicProcessor implements Processor<CreateTopicRequest, CreateTopicResponse> {
    public static final String CREATE_TOPIC_PROCESSOR_NAME = "CreateTopicProcessor";

    private final TopicManager topicManager = Broker.BROKER.getTopicManager();

    @Override
    public CreateTopicResponse process(RequestContext ctx, CreateTopicRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        String completeTopicName = topicDetail.getCompleteTopicName();

        Topic topic = topicManager.getTopic(completeTopicName);
        topic.getOrCreateSubscription(completeTopicName);
        return CreateTopicResponse
                .newBuilder()
                .setAck(true)
                .setRes("create success")
                .build();
    }

    public enum CreateTopicProcessorEnum {
        INSTANCE;
        private final CreateTopicProcessor createTopicProcessor;

        CreateTopicProcessorEnum() {
            createTopicProcessor = new CreateTopicProcessor();
        }

        public CreateTopicProcessor getInstance() {
            return createTopicProcessor;
        }
    }
}
