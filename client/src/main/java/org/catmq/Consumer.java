package org.catmq;

import org.catmq.remoting.netty.NettyClient;
import org.catmq.remoting.protocol.RemotingCommand;

public class Consumer {

    NettyClient client = new NettyClient();

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        consumer.client.start();
        // nothing echo
        RemotingCommand command = RemotingCommand.createRequestCommand(88, null);
        System.out.println("oneway id " + command.getRequestId());
        consumer.client.invokeOneway("127.0.0.1:8888", command, 5000);
        // sync call
        command = RemotingCommand.createRequestCommand(89, null);
        System.out.println("sync id " + command.getRequestId());
        RemotingCommand response = consumer.client.invokeSync("127.0.0.1:8888", command, 5000);
        System.out.println(response.getRemark());
        // async call
        command = RemotingCommand.createRequestCommand(90, null);
        System.out.println("async id " + command.getRequestId());
        consumer.client.invokeAsync("127.0.0.1:8888", command, 5000, future -> {
            if (future.isSendRequestOK()) {
                System.out.println("send done");
            }
        });

    }
}
