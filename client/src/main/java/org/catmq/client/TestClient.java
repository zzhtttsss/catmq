package org.catmq.client;

import org.catmq.client.common.MessageEntry;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.entity.TopicMode;
import org.catmq.entity.TopicType;
import org.catmq.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

public class TestClient {


    public static void main(String[] args) {
        CatClient client = CatClient.builder()
                .setTenantId("zzh")
                .setZkAddress("127.0.0.1:2181")
                .setProducerProxy(new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED))
                .build();


        client.createTopic("test", TopicType.PERSISTENT, TopicMode.NORMAL, 1);

        DefaultCatProducer producer = client.createProducer()
                .setTopic("persistent:normal:$zzh:test")
                .build();

        for (int i = 0; i < 100; i++) {
            List<MessageEntry> list = new ArrayList<>(10);
            for (int j = 0; j < 10; j++) {
                MessageEntry messageEntry = MessageEntry.builder()
                        .setBody(StringUtil.concatString("message, batch: ", String.valueOf(i), ", index: ", String.valueOf(j)).getBytes())
                        .build();
                list.add(messageEntry);
            }
            producer.sendMessage(list, 10000);
        }
        System.out.println("finish");
    }


}
