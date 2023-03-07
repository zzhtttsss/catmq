package org.catmq.broker.manager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.topic.Topic;
import org.catmq.broker.topic.nonpersistent.NonPersistentTopic;
import org.catmq.broker.topic.persistent.PersistentTopic;
import org.catmq.entity.TopicDetail;
import org.catmq.zk.ZkIdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.constant.StringConstant.DELAYED_MESSAGE_TOPIC_NAME;

@Slf4j
public class TopicManager {

    private final BrokerZkManager brokerZkManager;

    /**
     * All topics whose key is complete topicName
     */
    private final ConcurrentHashMap<String, Topic> topics;

    /**
     * The size of each segment.
     * Its key is segmentId and value is the total number of entries in this segment.
     * This info will be put into the zk.
     */
    private final ConcurrentHashMap<Long, AtomicLong> segmentSize;

    /**
     * The segmentId of each topic.
     * Its key is complete topicName and value is all segmentId in this topic.
     * Using List because there will be one thread adding segmentId.
     * This info will be put into the zk.
     */
    private final ConcurrentHashMap<String, List<Long>> topicSegmentIds;

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
        segmentSize.computeIfAbsent(segmentId, id -> new AtomicLong(0));
        topicSegmentIds.computeIfAbsent(topicDetail.getCompleteTopicName(),
                name -> new ArrayList<>()).add(segmentId);
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    /**
     * Add the segmentId of the topic when switching segment.
     */
    public void addSegmentId(String topicCompleteName, long segmentId) {
        topicSegmentIds.get(topicCompleteName).add(segmentId);
    }

    /**
     * Get the first segmentId of the topic when creating subscriptions.
     */
    public long getSegmentIdFirst(String topicCompleteName) {
        return topicSegmentIds.get(topicCompleteName).get(0);
    }

    public long getSegmentIdNext(String topicCompleteName, long segmentId) {
        List<Long> segmentIds = topicSegmentIds.get(topicCompleteName);
        int index = segmentIds.indexOf(segmentId);
        if (index == segmentIds.size() - 1) {
            // index is the last one which means the consumers have consumed all the entries
            return -1;
        }
        return segmentIds.get(index + 1);
    }

    /**
     * Get the number of entries in the specified segment.
     */
    public long getSegmentTotalNumber(long segmentId) {
        return segmentSize.get(segmentId).get();
    }

    public void addSegmentNumber(long segmentId) {
        addSegmentNumber(segmentId, 1L);
    }

    public void addSegmentNumber(long segmentId, long delta) {
        segmentSize.putIfAbsent(segmentId, new AtomicLong(0));
        segmentSize.get(segmentId).addAndGet(delta);
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
        segmentSize = new ConcurrentHashMap<>();
        topicSegmentIds = new ConcurrentHashMap<>();
        brokerZkManager = BROKER.getBrokerZkManager();
        long segmentId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(brokerZkManager.client);
        createPartition(DELAYED_MESSAGE_TOPIC_NAME, segmentId);
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
