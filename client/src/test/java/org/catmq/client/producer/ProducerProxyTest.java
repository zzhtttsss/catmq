package org.catmq.client.producer;

import org.catmq.client.producer.balance.LeastUsedStrategy;
import org.junit.Assert;
import org.junit.Test;

public class ProducerProxyTest {

    @Test
    public void testProducerProxy() {
        ProducerProxy pp = new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED);
        Assert.assertTrue(pp.getLoadBalance() instanceof LeastUsedStrategy);
    }
}
