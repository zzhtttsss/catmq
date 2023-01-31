package org.catmq.pipline.processor;

import com.google.protobuf.ByteString;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.manager.ClientManager;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.GetMessageFromBrokerRequest;
import org.catmq.protocol.service.GetMessageFromBrokerResponse;

import java.util.Arrays;
import java.util.Optional;

public class GetMessageProcessor implements Processor<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse> {

    public static final String CONSUME_PROCESSOR_NAME = "ConsumeProcessor";

    private final TopicManager topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();
    private final ClientManager clientManager = ClientManager.ClientManagerEnum.INSTANCE.getInstance();

    @Override
    public GetMessageFromBrokerResponse process(RequestContext ctx, GetMessageFromBrokerRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        Topic topic = topicManager.getTopic(topicDetail.getCompleteTopicName());
        if (!topic.isSubscribe(topicDetail.getCompleteTopicName(), request.getConsumerId())) {
            topic.subscribe(topicDetail.getCompleteTopicName(), request.getConsumerId());
        }
        Consumer consumer = clientManager.getConsumer(request.getConsumerId());
        Optional<byte[]> message = consumer.getMessage();
        return message
                .map(s -> GetMessageFromBrokerResponse
                        .newBuilder()
                        .setAck(true)
                        .setRes(Arrays.toString(s))
                        .setMessage(ByteString.copyFrom(s))
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
