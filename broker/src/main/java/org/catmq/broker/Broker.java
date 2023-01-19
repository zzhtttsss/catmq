package org.catmq.broker;

import lombok.Getter;

/**
 * Broker with every service
 */
public class Broker {
  

    public enum BrokerEnum {
        INSTANCE;
        @Getter
        private Broker broker;

        BrokerEnum() {
            broker = new Broker();
        }
    }
}




