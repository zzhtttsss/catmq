package org.catmq.pipline.processor;

import org.catmq.broker.Broker;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SubscribeRequest;
import org.catmq.protocol.service.SubscribeResponse;

public class SubscribeProcessor implements Processor<SubscribeRequest, SubscribeResponse> {

    private final TopicManager topicManager = Broker.BROKER.getTopicManager();

    @Override
    public SubscribeResponse process(RequestContext ctx, SubscribeRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        String subscriptionName = request.getSubscription();
        String completeName = topicDetail.getCompleteTopicName();
        // Get topic
        Topic topic = topicManager.getTopic(completeName);
        topic.subscribe(subscriptionName, ctx.getConsumerId());
        return SubscribeResponse
                .newBuilder()
                .setAck(true)
                .setRes("subscribe success")
                .build();
    }


    public enum SubscribeProcessorEnum {
        INSTANCE;
        private final SubscribeProcessor instance = new SubscribeProcessor();

        public SubscribeProcessor getInstance() {
            return instance;
        }
    }
}
