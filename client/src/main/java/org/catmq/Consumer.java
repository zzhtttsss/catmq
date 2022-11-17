package org.catmq;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.catmq.remoting.netty.NettyDecoder;
import org.catmq.remoting.netty.NettyEncoder;
import org.catmq.remoting.protocol.RemotingCommand;

import java.net.InetSocketAddress;

public class Consumer {

    EventLoopGroup group;

    public void connect() {
        Bootstrap client = new Bootstrap();
        group = new NioEventLoopGroup();
        client.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress("localhost", 8080))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast("decoder", new NettyDecoder())
                                .addLast("encoder", new NettyEncoder())
                                .addLast(new SimpleClientHandler());
                    }
                });
        try {
            ChannelFuture channelFuture = client.connect().sync();
            Channel channel = channelFuture.channel();
            System.out.println("-------" + channel.localAddress() + "--------");
            System.out.println("---------Send a cmd----------");
            RemotingCommand request = RemotingCommand.createRequestCommand(88, null);
            channel.writeAndFlush(request);
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }


    }

    public static void main(String[] args) {
        new Consumer().connect();
    }
}
