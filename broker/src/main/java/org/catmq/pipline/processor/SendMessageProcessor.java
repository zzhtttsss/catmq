package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class SendMessageProcessor implements Processor<SendMessage2BrokerRequest, SendMessage2BrokerResponse> {

    public static final String PRODUCE_PROCESSOR_NAME = "ProduceProcessor";
    private final TopicManager topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();

    @Override
    public SendMessage2BrokerResponse process(RequestContext ctx, SendMessage2BrokerRequest request) {
        /**
         * 1. 获取到topic信息，获取当前segment所处的多个storer的zk路径，给消息添加segmentId entryId （对于高可用性，一批消息可能出现当前segment只能放入一部分消息，
         * 其他要放到下一个segment中）
         * 2. 将批量消息异步并发发给每一个storer，整理结果
         **/

        TopicDetail topicDetail = TopicDetail.get(request.getTopic());
        Topic topic = BROKER.getTopicManager().getTopic(topicDetail.getCompleteTopicName());
//        topic.getOrCreateSubscription(topicDetail.getCompleteTopicName());
        topic.putMessage(request.getMessageList());
        return SendMessage2BrokerResponse
                .newBuilder()
                .setAck(true)
                .setRes("send success")
                .build();
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
