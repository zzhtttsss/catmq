package org.catmq.client;

import org.catmq.client.common.MessageEntry;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.util.StringUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestClient {


    public static void main(String[] args) {
        CatClient client = CatClient.builder()
                .setTenantId("zzh")
                .setZkAddress("127.0.0.1:2181")
                .setProducerProxy(new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED))
                .build();
        CountDownLatch countDownLatch = new CountDownLatch(20);


        List<Long> timeList = Collections.synchronizedList(new ArrayList<>(100000));
        String test = "CatDFS is mainly independently designed and implemented from scratch by two master students " +
                "(who are also noob software engineers)@zzhtttsss and @DividedMoon. Our purpose is mainly to exercise " +
                "our ability to independently design and implement projects, and to be familiar with distributed systems," +
                " Raft algorithms and various dependent components. This is the first time we released an independently " +
                "implemented project on GitHub, and we are also noobs. So during the development process, the solutions " +
                "to problems encountered are limited, and there are still many bugs or bad designs in the CatDFS. Feel " +
                "free to make suggestions and questions about CatDFS. We hope CatDFS can help others to learn the " +
                "distributed file system, and we will continue improving CatDFS in the future~";

        client.createTopic("test", "persistent", "normal", 1);

        DefaultCatProducer producer0 = client.createProducer("persistent:normal:$zzh:test")
                .build();
        for (int i = 0; i < 1000; i++) {
            List<MessageEntry> list = new ArrayList<>(10);
            for (int j = 0; j < 10; j++) {
                MessageEntry messageEntry = MessageEntry.builder()
                        .setBody(StringUtil.concatString("message, batch: ", String.valueOf(i), ", index: ", String.valueOf(j), test).getBytes())
                        .build();
                list.add(messageEntry);
            }
            producer0.sendMessage(list, 10000);


//            MessageEntry messageEntry = MessageEntry.builder()
//                    .setBody(StringUtil.concatString("message, thread:", String.valueOf(i), test).getBytes())
//                    .build();
//            producer0.sendMessage(messageEntry, 10000);
        }


        long start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
            int index = i;
            new Thread(() -> {
                client.createTopic("test" + index, "persistent", "normal", 1);

                DefaultCatProducer producer = client.createProducer("persistent:normal:$zzh:test" + index)
                        .build();
                for (int j = 0; j < 1000; j++) {
                    List<MessageEntry> list = new ArrayList<>(10);
                    for (int k = 0; k < 10; k++) {
                        MessageEntry messageEntry = MessageEntry.builder()
                                .setBody(StringUtil.concatString("message, thread:", String.valueOf(index),
                                        ", batch: ", String.valueOf(j), ", index: ", String.valueOf(k), test).getBytes())
                                .build();
                        list.add(messageEntry);
                    }
                    long startTime = System.currentTimeMillis();
                    producer.sendMessage(list, 10000);

//                    long startTime = System.currentTimeMillis();
//                    MessageEntry messageEntry = MessageEntry.builder()
//                            .setBody(StringUtil.concatString("message, thread:", String.valueOf(index),
//                                    ", batch: ", String.valueOf(j), test).getBytes())
//                            .build();
//                    producer.sendMessage(messageEntry, 10000);

                    timeList.add(System.currentTimeMillis() - startTime);
                }
                countDownLatch.countDown();
            }).start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();

        double avg = timeList.stream().mapToLong(Long::valueOf).average().getAsDouble();
//        long p50 = timeList.stream().sorted().limit(100000).toList().get(99999);
//        long p90 = timeList.stream().sorted().limit(180000).toList().get(179999);
//        long p95 = timeList.stream().sorted().limit(190000).toList().get(189999);
        long p50 = timeList.stream().sorted().limit(10000).toList().get(9999);
        long p90 = timeList.stream().sorted().limit(18000).toList().get(17999);
        long p95 = timeList.stream().sorted().limit(19000).toList().get(18999);
        System.out.println("total cost: " + (end - start));
        System.out.println("avg: " + avg);
        System.out.println("p50: " + p50);
        System.out.println("p90: " + p90);
        System.out.println("p95: " + p95);
        System.out.println("TPS: " + 20000 * 1000 / (end - start));
        System.out.println("finish");


    }


}
