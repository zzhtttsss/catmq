package org.catmq.broker.service;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.manager.StorerManager;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Topic;
import org.catmq.collection.DelayedMessageIndex;
import org.catmq.collection.TimerTaskList;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.service.GetMessageFromStorerResponse;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.thread.ServiceThread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class HandleDelayedMessageService extends ServiceThread {
    private final StorerManager storerManager = BROKER.getStorerManager();
    private final TopicManager topicManager = BROKER.getTopicManager();

    private final BlockingQueue<List<TimerTaskList.TimerTaskEntry>> expireDelayedMessageQueue;


    public HandleDelayedMessageService(BlockingQueue<List<TimerTaskList.TimerTaskEntry>> expireDelayedMessageQueue) {
        this.expireDelayedMessageQueue = expireDelayedMessageQueue;
    }

    @Override
    public String getServiceName() {
        return HandleDelayedMessageService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("{} service started.", this.getServiceName());
        while (!this.isStopped()) {
            try {
                List<TimerTaskList.TimerTaskEntry> expireDelayedMessageList = expireDelayedMessageQueue.take();
                List<CompletableFuture<HandleResult>> handleResultFutureList = new ArrayList<>();
                for (TimerTaskList.TimerTaskEntry timerTaskEntry : expireDelayedMessageList) {
                    CompletableFuture<HandleResult> handleResultFuture = handleExpireDelayedMessage(timerTaskEntry.getDelayedMessageIndex());
                    handleResultFutureList.add(handleResultFuture);
                }
                CompletableFuture<Void> all = CompletableFuture.allOf(handleResultFutureList.toArray(new CompletableFuture[0]));
                all.get();
            } catch (InterruptedException e) {
                log.warn("Interrupted!", e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("{} service end.", this.getServiceName());
    }

    private CompletableFuture<HandleResult> handleExpireDelayedMessage(DelayedMessageIndex delayedMessageIndex) {
        return storerManager
                .getMessageFromStorer(delayedMessageIndex.getSegmentId(), delayedMessageIndex.getEntryId(), new String[]{"127.0.0.1:4321"})
                .thenCompose(getMessageFromStorerResponse -> sendMessage2RealTopic(delayedMessageIndex, getMessageFromStorerResponse))
                .thenApply(sendMessage2BrokerResponse -> {
                    if (sendMessage2BrokerResponse.getStatus().getCode() != Code.OK) {
                        return new HandleResult(true, false);
                    } else {
                        return new HandleResult(false, true);
                    }
                }).exceptionally(throwable -> {
                    log.error("handleExpireDelayedMessage error.", throwable);
                    return new HandleResult(false, true);
                });
    }

    private CompletableFuture<SendMessage2BrokerResponse> sendMessage2RealTopic(
            DelayedMessageIndex delayedMessageIndex, GetMessageFromStorerResponse getMessageFromStorerResponse) {
        log.warn("sendMessage2RealTopic topic: {}.", delayedMessageIndex.getCompleteTopic());
        TopicDetail topicDetail = TopicDetail.get(delayedMessageIndex.getCompleteTopic());
        Topic topic = topicManager.getTopic(topicDetail.getCompleteTopicName());
        List<OriginMessage> messageList = new ArrayList<>();
        for (NumberedMessage msg : getMessageFromStorerResponse.getMessageList()) {
            //flush message by resetting the offset
            messageList.add(OriginMessage.newBuilder().setBody(msg.getBody()).build());
        }
        return topic.putMessage(messageList);
    }

    static class HandleResult {
        private final boolean success;
        private final boolean needRetry;

        public HandleResult(boolean success, boolean needRetry) {
            this.success = success;
            this.needRetry = needRetry;
        }

        public boolean isSuccess() {
            return success;
        }

        public boolean isNeedRetry() {
            return needRetry;
        }
    }
}
