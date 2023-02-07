package org.catmq.broker.manager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.topic.Topic;
import org.catmq.broker.topic.nonpersistent.NonPersistentTopic;
import org.catmq.broker.topic.persistent.PersistentTopic;
import org.catmq.entity.TopicDetail;
import org.catmq.zk.ZkIdGenerator;

import java.util.concurrent.ConcurrentHashMap;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class TopicManager {

    private final BrokerZkManager brokerZkManager;

    /**
     * All topics whose key is complete topicName
     */
    private final ConcurrentHashMap<String, Topic> topics;

    public void createPartition(String topicName, long segmentId) {
        TopicDetail topicDetail = TopicDetail.get(topicName);
        brokerZkManager.createPartition(topicDetail, brokerZkManager.getBrokerPath());
        topics.computeIfAbsent(topicDetail.getCompleteTopicName(), name -> {
            if (topicDetail.isPersistent()) {
                return new PersistentTopic(topicDetail, segmentId);
            } else {
                return new NonPersistentTopic(topicDetail);
            }
        });
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    /**
     * Whether the topic exists
     *
     * @param topicName the complete topic name
     * @return true if the topic exists
     */
    public boolean containsTopic(String topicName) {
        return topics.containsKey(topicName);
    }

    private TopicManager() {
        topics = new ConcurrentHashMap<>();
        brokerZkManager = BROKER.getBrokerZkManager();
        long segmentId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(brokerZkManager.client);
        createPartition("persistent:normal:$catmq:delayMessage", segmentId);
    }

    public enum TopicManagerEnum {
        INSTANCE();
        @Getter
        private final TopicManager instance;

        TopicManagerEnum() {
            this.instance = new TopicManager();
        }
    }
}
