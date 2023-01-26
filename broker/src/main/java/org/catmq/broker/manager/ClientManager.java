package org.catmq.broker.manager;

import lombok.Getter;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.Producer;

import java.util.concurrent.ConcurrentHashMap;

public class ClientManager {

    private final ConcurrentHashMap<Long, Producer> producers;
    private final ConcurrentHashMap<Long, Consumer> consumers;

    public void addProducer(Producer producer) {
        producers.put(producer.getProducerId(), producer);
    }

    public void addConsumer(Consumer consumer) {
        consumers.put(consumer.getConsumerId(), consumer);
    }

    public Consumer getConsumer(long consumerId) {
        return consumers.get(consumerId);
    }

    public Producer getProducer(long producerId) {
        return producers.get(producerId);
    }

    public boolean isProducerExist(long producerId) {
        return producers.containsKey(producerId);
    }

    public boolean isConsumerExist(long consumerId) {
        return consumers.containsKey(consumerId);
    }

    private ClientManager() {
        producers = new ConcurrentHashMap<>();
        consumers = new ConcurrentHashMap<>();
    }

    public enum ClientManagerEnum {
        INSTANCE();
        @Getter
        private final ClientManager instance;

        ClientManagerEnum() {
            this.instance = new ClientManager();
        }
    }
}
