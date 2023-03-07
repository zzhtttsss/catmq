package org.catmq.client;

import org.catmq.client.common.MessageEntry;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

public class TestProducer {

    public static void main(String[] args) {
        CatClient client = CatClient.builder()
                .setTenantId("zzh")
                .setZkAddress("127.0.0.1:2181")
                .setProducerProxy(new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED))
                .build();
        String test = "CatDFS is mainly independently designed and implemented from scratch by two master students " +
                "(who are also noob software engineers)@zzhtttsss and @DividedMoon. Our purpose is mainly to exercise " +
                "our ability to independently design and implement projects, and to be familiar with distributed systems," +
                " Raft algorithms and various dependent components. This is the first time we released an independently " +
                "implemented project on GitHub, and we are also noobs. So during the development process, the solutions " +
                "to problems encountered are limited, and there are still many bugs or bad designs in the CatDFS. Feel " +
                "free to make suggestions and questions about CatDFS. We hope CatDFS can help others to learn the " +
                "distributed file system, and we will continue improving CatDFS in the future~";

        client.createTopic("test", "persistent", "normal", 1);
        String topicName = "persistent:normal:$zzh:test";


        DefaultCatProducer producer0 = client.createProducer(topicName).build();
        for (int i = 0; i < 10; i++) {
            List<MessageEntry> list = new ArrayList<>(10);
            for (int j = 0; j < 10; j++) {
                MessageEntry messageEntry = MessageEntry.builder()
                        .setBody(StringUtil.concatString("message, batch: ", String.valueOf(i), ", index: ", String.valueOf(j), test).getBytes())
                        .build();
                list.add(messageEntry);
            }
            producer0.sendMessage(list, 10000);
        }
        client.close();
    }
}
