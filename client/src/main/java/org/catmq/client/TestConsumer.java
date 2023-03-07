package org.catmq.client;

import org.catmq.client.producer.ProducerProxy;
import org.catmq.entity.ConsumerBatchPolicy;
import org.catmq.protocol.definition.NumberedMessage;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class TestConsumer {
    public static void main(String[] args) {
        CatClient client = CatClient.builder()
                .setTenantId("zzh")
                .setZkAddress("127.0.0.1:2181")
                .setProducerProxy(new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED))
                .build();
        String topicName = "persistent:normal:$zzh:test";
        String subscriptionName = "byl";
        DefaultCatConsumer consumer0 = client.
                createConsumer(topicName, subscriptionName).
                setBatchPolicy(ConsumerBatchPolicy.ConsumerBatchPolicyBuilder.builder()
                        .setBatchNumber(10)
                        .setTimeoutInMs(1000)
                        .build()).
                build();
        consumer0.subscribe();
        for (int i = 0; i < 10; i++) {
            consumer0.getMessage((res) -> {
                for (NumberedMessage msg : res.getMessageList()) {
                    System.out.println(msg.getBody().toString(StandardCharsets.UTF_8));
                }
            }, 10, TimeUnit.SECONDS);
        }
        client.close();
    }
}
