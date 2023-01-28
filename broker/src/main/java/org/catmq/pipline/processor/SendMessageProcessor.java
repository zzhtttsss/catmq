package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class SendMessageProcessor implements Processor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";
    private final TopicManager topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();

    @Override
    public SendMessage2BrokerResponse process(RequestContext ctx, SendMessage2BrokerRequest request) {
        SendMessage2BrokerResponse response;
        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        Topic topic = BROKER.getTopicManager().getTopic(topicDetail.getCompleteTopicName());
        var messageList = request.getMessageList();
        CompletableFuture<SendMessage2BrokerResponse> future = topic.putMessage(messageList);
        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return response;
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
