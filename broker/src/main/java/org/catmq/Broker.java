package org.catmq;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.catmq.remoting.netty.NettyDecoder;
import org.catmq.remoting.netty.NettyEncoder;

public class Broker {
    EventLoopGroup parentGroup;
    EventLoopGroup childGroup;

    public static void main(String[] args) {
        new Broker().serve();
    }

    public void serve() {
        ServerBootstrap server = new ServerBootstrap();
        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup();
        server.group(parentGroup, childGroup)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast("decoder", new NettyDecoder())
                                .addLast("encoder", new NettyEncoder())
                                .addLast(new SimpleServerHandler());
                    }
                });

        System.out.println("server start up ....");
        try {
            ChannelFuture future = server.bind(8080).sync();
            future.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess())
                        System.out.println("addr " + future.channel().localAddress());
                }
            });
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            parentGroup.shutdownGracefully();
            childGroup.shutdownGracefully();
        }
    }
}
