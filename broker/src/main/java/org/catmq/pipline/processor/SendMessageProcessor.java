package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.service.ScheduleDelayedMessageService;
import org.catmq.broker.topic.Topic;
import org.catmq.collection.DelayedMessageIndex;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.constant.StringConstant.DELAYED_MESSAGE_TOPIC_NAME;

@Slf4j
public class SendMessageProcessor implements Processor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";
    private final TopicManager topicManager = BROKER.getTopicManager();
    private final ScheduleDelayedMessageService scheduleDelayedMessageService = BROKER.getScheduleDelayedMessageService();

    @Override
    public SendMessage2BrokerResponse process(RequestContext ctx, SendMessage2BrokerRequest request) {
        SendMessage2BrokerResponse response;
        String topicName = request.getTopic();
        long expireTime = request.getMessage(0).getExpireTime();
        boolean isDelayMessage = expireTime > System.currentTimeMillis();
        if (isDelayMessage) {
            if (expireTime > System.currentTimeMillis() + 1000 * 60 * 60 * 24) {
                throw new RuntimeException("delay time is too long");
            }
            topicName = DELAYED_MESSAGE_TOPIC_NAME;
        }
        TopicDetail topicDetail = TopicDetail.get(topicName);
        Topic topic = topicManager.getTopic(topicDetail.getCompleteTopicName());
        var messageList = request.getMessageList();
        CompletableFuture<SendMessage2BrokerResponse> future = topic.putMessage(messageList);
        try {
            response = future.get();
            if (isDelayMessage) {
                scheduleDelayedMessageService.getDelayedMessageTimer().
                        add(new DelayedMessageIndex(expireTime, request.getTopic(),
                                response.getSegmentId(), response.getFirstEntryId()));
            }
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
