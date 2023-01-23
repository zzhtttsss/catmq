package org.catmq.pipline.processor;

import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.Topic;
import org.catmq.common.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.CreateTopicRequest;
import org.catmq.protocol.service.CreateTopicResponse;

public class CreateTopicProcessor implements Processor<CreateTopicRequest, CreateTopicResponse> {
    public static final String CREATE_TOPIC_PROCESSOR_NAME = "CreateTopicProcessor";

    private final TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();

    @Override
    public CreateTopicResponse process(RequestContext ctx, CreateTopicRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        String completeTopicName = topicDetail.getCompleteTopicName();
        if (!topicService.containsTopic(completeTopicName)) {
            topicService.createTopic(completeTopicName, ctx.getBrokerPath());
        }
        Topic topic = topicService.getTopic(completeTopicName);
        //TODO: default subscription name
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
