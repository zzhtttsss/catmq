package org.catmq.broker.service;

import lombok.Getter;
import org.catmq.broker.topic.ITopic;
import org.catmq.broker.topic.TopicName;
import org.catmq.broker.topic.nonpersistent.NonPersistentTopic;

import java.util.concurrent.ConcurrentHashMap;

public class TopicService {

    private final ZkService zkService;

    /**
     * All topics whose key is complete topicName
     */
    private final ConcurrentHashMap<String, ITopic> topics;

    /**
     * Add topic interface whose name is complete topicName to the topic list.
     * And then, create the topic in zookeeper.
     *
     * @param topicName  topic name
     * @param brokerPath broker path like /broker/[brokerName]
     */
    public void createTopic(String topicName, String brokerPath) {
        TopicName topic = TopicName.get(topicName);
        topics.computeIfAbsent(topic.getCompleteTopicName(), name -> {
            if (topic.isPersistent()) {
                throw new UnsupportedOperationException("Persistent topic is not supported yet");
            } else {
                zkService.createTopic(topic.getCompleteTopicName(), brokerPath);
                return new NonPersistentTopic(topic.getCompleteTopicName());
            }
        });
    }

    public ITopic getTopic(String topicName) {
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

    private TopicService() {
        topics = new ConcurrentHashMap<>();
        zkService = ZkService.ZkServiceEnum.INSTANCE.getInstance();
    }

    public enum TopicServiceEnum {
        INSTANCE();
        @Getter
        private final TopicService instance;

        TopicServiceEnum() {
            this.instance = new TopicService();
        }
    }
}
