package org.catmq.entity;

import org.junit.Assert;
import org.junit.Test;

public class TopicDetailTest {
    @Test
    public void testGetWithSimpleTopic() {
        String topic = "test";
        TopicDetail detail = TopicDetail.get(topic);
        Assert.assertEquals("non-persistent:normal:$public:test", detail.getCompleteTopicName());
    }

    @Test
    public void testGetWithCompleteTopic() {
        String topic = "non-persistent:normal:$public:test";
        TopicDetail detail = TopicDetail.get(topic);
        Assert.assertEquals(TopicType.NON_PERSISTENT, detail.getType());
        Assert.assertEquals(TopicMode.NORMAL, detail.getMode());
        Assert.assertEquals("test", detail.getSimpleName());
        Assert.assertEquals("public", detail.getTenant());
    }
}
