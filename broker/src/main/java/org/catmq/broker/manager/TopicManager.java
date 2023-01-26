package org.catmq.broker.manager;

import lombok.Getter;
import org.catmq.broker.topic.Topic;
import org.catmq.broker.topic.persistent.PersistentTopic;
import org.catmq.entity.TopicDetail;
import org.catmq.broker.topic.nonpersistent.NonPersistentTopic;

import java.util.concurrent.ConcurrentHashMap;

import static org.catmq.broker.Broker.BROKER;

public class TopicManager {

    private final BrokerZkManager brokerZkManager;

    /**
     * All topics whose key is complete topicName
     */
    private final ConcurrentHashMap<String, Topic> topics;

    public void createPartition(String topicName, String brokerPath) {
        TopicDetail topicDetail = TopicDetail.get(topicName);
        brokerZkManager.createPartition(topicDetail, brokerPath);
        topics.computeIfAbsent(topicDetail.getCompleteTopicName(), name -> {
            if (topicDetail.isPersistent()) {
                return new PersistentTopic(topicDetail);
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
