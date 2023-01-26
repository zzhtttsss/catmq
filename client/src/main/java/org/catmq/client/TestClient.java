package org.catmq.client;

import org.catmq.client.common.MessageEntry;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.entity.TopicMode;
import org.catmq.entity.TopicType;

public class TestClient {



    public static void main(String[] args) {
        CatClient client = CatClient.builder()
                .setTenantId("zzh")
                .setZkAddress("127.0.0.1:2181")
                .setProducerProxy(new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED))
                .build();

        DefaultCatProducer producer = client.createProducer()
                .setTopic("test")
                .build();

        client.createTopic("test", TopicType.NON_PERSISTENT, TopicMode.NORMAL, 1);

        MessageEntry messageEntry = MessageEntry.builder()
                .setBody("hello world".getBytes())
                .setProperties("key", "value")
                .build();



    }


}
