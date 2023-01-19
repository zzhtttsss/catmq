package org.catmq.broker.topic;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.catmq.constant.FileConstant;
import org.catmq.util.StringUtil;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Getter
@Slf4j
public class TopicName {
    public static final String PUBLIC_TENANT = "public";

    public static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private static final String TOPIC_DOMAIN_SEPARATOR = "://";

    // full name of topic
    // <topicType>://<tenant>/<topic>
    private final String completeTopicName;

    private final TopicType type;
    private final String tenant;
    private final String localName;

    private final int partitionIndex;

    private static final LoadingCache<String, TopicName> CACHE = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public @NonNull TopicName load(@NonNull String name) {
                    return new TopicName(name);
                }
            });

    public static TopicName get(String domain, String topic) {
        String name = StringUtil.concatString(domain, TOPIC_DOMAIN_SEPARATOR, PUBLIC_TENANT,
                FileConstant.LEFT_SLASH, topic);
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String tenant, String topic) {
        String name = StringUtil.concatString(domain, TOPIC_DOMAIN_SEPARATOR, tenant,
                FileConstant.LEFT_SLASH, topic);
        return TopicName.get(name);
    }

    public static TopicName get(String topic) {
        try {
            return CACHE.get(topic);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Whether the basic topic name is valid.
     * Should be updated to delete outdated keys.
     *
     * @param topic basic topic name
     * @return true if valid
     */
    public static boolean containsKey(String topic) {
        return Optional.of(topic).map(CACHE::getIfPresent).isPresent();
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public static int getPartitionIndex(String topic) {
        int partitionIndex = -1;
        if (topic.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                String idx = StringUtil.substringAfterLast(topic, PARTITIONED_TOPIC_SUFFIX);
                partitionIndex = Integer.parseInt(idx);
                if (partitionIndex < 0) {
                    // for the "topic-partition--1"
                    partitionIndex = -1;
                } else if (idx.length() != String.valueOf(partitionIndex).length()) {
                    // for the "topic-partition-01"
                    partitionIndex = -1;
                }
            } catch (NumberFormatException e) {
                // ignore exception
            }
        }
        return partitionIndex;
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public int getPartitionIndex() {
        return partitionIndex;
    }

    public boolean isPartitioned() {
        return partitionIndex != -1;
    }


    /**
     * For partitions in a topic, return the base partitioned topic name.
     * Eg:
     * <ul>
     *  <li><code>persistent://prop/my-topic-partition-1</code> -->
     *  <code>persistent://prop/my-topic</code>
     *  <li><code>persistent://prop/my-topic</code> -->
     *  <code>persistent://prop/my-topic</code>
     * </ul>
     *
     * @return the base partitioned topic name without partition index.
     */
    public String getPartitionedTopicName() {
        if (isPartitioned()) {
            return completeTopicName.substring(0, completeTopicName.lastIndexOf("-partition-"));
        } else {
            return completeTopicName;
        }
    }

    public boolean isPersistent() {
        return type == TopicType.PERSISTENT;
    }

    /**
     * Create a topic name from a string.
     *
     * @param name long type: [TopicType]://[tenant]/[namespace]/[localName]<br/>
     *             short type: [localName]
     */
    private TopicName(String name) {
        log.info("create a new topic named {}", name);
        if (!name.contains(TOPIC_DOMAIN_SEPARATOR)) {
            // short name like <topic> with default TopicType.NON_PERSISTENT and default tenant
            // non-persistent://public/<name>
            this.type = TopicType.NON_PERSISTENT;
            this.tenant = PUBLIC_TENANT;
            this.localName = name;
            this.completeTopicName = StringUtil.concatString(TopicType.NON_PERSISTENT.getName(),
                    TOPIC_DOMAIN_SEPARATOR, PUBLIC_TENANT,
                    FileConstant.LEFT_SLASH, name);
            this.partitionIndex = getPartitionIndex(name);

        } else {
            // long name like persistent://tenant/topic
            List<String> parts = Splitter.on(TOPIC_DOMAIN_SEPARATOR).limit(2).splitToList(name);
            this.type = TopicType.fromString(parts.get(0));
            String rest = parts.get(1);
            // The rest of the name is like:
            // new:    tenant/<topic>
            parts = Splitter.on(FileConstant.LEFT_SLASH).limit(2).splitToList(rest);
            if (parts.size() == 2) {
                this.tenant = parts.get(0);
                this.localName = parts.get(1);
                this.completeTopicName = name;
                this.partitionIndex = getPartitionIndex(name);
            } else {
                throw new IllegalArgumentException("Invalid topic name: " + name);
            }
        }
        if (StringUtil.isEmpty(this.localName)) {
            throw new IllegalArgumentException("Invalid topic name: " + completeTopicName);
        }
    }
}
