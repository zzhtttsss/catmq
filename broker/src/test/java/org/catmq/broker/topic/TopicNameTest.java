package org.catmq.broker.topic;

import org.junit.Assert;
import org.junit.Test;

public class TopicNameTest {
    @Test
    public void testTopicType() {
        String domain = "persistent";
        Assert.assertEquals(TopicType.PERSISTENT, TopicType.fromString(domain));
        domain = "non-persistent";
        Assert.assertEquals(TopicType.NON_PERSISTENT, TopicType.fromString(domain));
        final String finalDomain = "nonpersistent";
        Assert.assertThrows(IllegalArgumentException.class, () -> TopicType.fromString(finalDomain));
    }
}
