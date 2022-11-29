package org.catmq;

import org.catmq.remoting.netty.NettyServer;

/**
 * @author BYL
 */
public class Broker {
    NettyServer server = new NettyServer();

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.server.start();
    }
}
