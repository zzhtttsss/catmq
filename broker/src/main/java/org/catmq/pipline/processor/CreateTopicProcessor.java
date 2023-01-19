package org.catmq.processor;

import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.ITopic;
import org.catmq.broker.topic.TopicName;
import org.catmq.context.RequestContext;
import org.catmq.protocol.service.CreateTopicRequest;
import org.catmq.protocol.service.CreateTopicResponse;

public class CreateTopicProcessor implements Processor<CreateTopicRequest, CreateTopicResponse> {
    public static final String CREATE_TOPIC_PROCESSOR_NAME = "CreateTopicProcessor";

    private final TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();

    @Override
    public CreateTopicResponse process(RequestContext ctx, CreateTopicRequest request) {
        TopicName topicName = TopicName.get(request.getTopic());
        String completeTopicName = topicName.getCompleteTopicName();
        if (!topicService.containsTopic(completeTopicName)) {
            topicService.createTopic(completeTopicName, ctx.getBrokerPath());
        }
        ITopic topic = topicService.getTopic(completeTopicName);
        //TODO: default subscription name
        topic.createSubscription(completeTopicName);
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
