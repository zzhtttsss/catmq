package org.catmq.pipline.processor;

import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.Topic;
import org.catmq.common.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;

public class CreatePartitionProcessor implements Processor<CreatePartitionRequest, CreatePartitionResponse> {
    @Override
    public CreatePartitionResponse process(RequestContext ctx, CreatePartitionRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        String completeTopicName = topicDetail.getCompleteTopicName();
        TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();
        topicService.createPartition(completeTopicName, ctx.getBrokerPath());
        return CreatePartitionResponse
                .newBuilder()
                .setAck(true)
                .setRes("create success")
                .build();
    }

    public enum CreatePartitionProcessorEnum {
        INSTANCE;
        private final CreatePartitionProcessor instance;

        CreatePartitionProcessorEnum() {
            instance = new CreatePartitionProcessor();
        }

        public CreatePartitionProcessor getInstance() {
            return instance;
        }
    }
}
