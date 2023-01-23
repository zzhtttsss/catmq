package org.catmq.pipline.processor;

import com.google.protobuf.ByteString;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.service.ClientManageService;
import org.catmq.broker.service.TopicService;
import org.catmq.broker.topic.Topic;
import org.catmq.common.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.GetMessageFromBrokerRequest;
import org.catmq.protocol.service.GetMessageFromBrokerResponse;

import java.util.Arrays;
import java.util.Optional;

public class GetMessageProcessor implements Processor<GetMessageFromBrokerRequest, GetMessageFromBrokerResponse> {

    public static final String CONSUME_PROCESSOR_NAME = "ConsumeProcessor";

    private final TopicService topicService = TopicService.TopicServiceEnum.INSTANCE.getInstance();
    private final ClientManageService clientManageService = ClientManageService.ClientManageServiceEnum.INSTANCE.getInstance();

    @Override
    public GetMessageFromBrokerResponse process(RequestContext ctx, GetMessageFromBrokerRequest request) {
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        Topic topic = topicService.getTopic(topicDetail.getCompleteTopicName());
        if (!topic.isSubscribe(topicDetail.getCompleteTopicName(), request.getConsumerId())) {
            topic.subscribe(topicDetail.getCompleteTopicName(), request.getConsumerId());
        }
        Consumer consumer = clientManageService.getConsumer(request.getConsumerId());
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
