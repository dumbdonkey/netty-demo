package com.dumb;

import static io.netty.buffer.Unpooled.copiedBuffer;

import com.google.common.base.Strings;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

public class NettyHttpServer {

    private ChannelFuture channel;
    private final EventLoopGroup masterGroup;
    private final EventLoopGroup slaveGroup;
    private final int port;
    private final int numberOfSlave;
    private final int numberOfBacklog;
    private final String mode;

    public NettyHttpServer(String mode, String port, String numberOfSlave, String numberOfBacklog) {
        this.port = Strings.isNullOrEmpty(port) ? 8080 : Integer.parseInt(port);
        this.numberOfBacklog = Strings.isNullOrEmpty(numberOfBacklog) ? 128 : Integer.parseInt(numberOfBacklog);
        this.numberOfSlave = Strings.isNullOrEmpty(numberOfSlave) ? NettyRuntime.availableProcessors() * 2
            : Integer.parseInt(numberOfSlave);
        this.mode = mode;
        if (mode.equalsIgnoreCase("oio")) {
            masterGroup = new OioEventLoopGroup();
            slaveGroup = new OioEventLoopGroup(this.numberOfSlave,
                new BasicThreadFactory.Builder().namingPattern("slave-%d").build());
        } else if (mode.equalsIgnoreCase("nio")) {
            masterGroup = new NioEventLoopGroup();
            slaveGroup = new NioEventLoopGroup(this.numberOfSlave,
                new BasicThreadFactory.Builder().namingPattern("slave-%d").build());
        } else if (mode.equalsIgnoreCase("nnio")) {
            masterGroup = new EpollEventLoopGroup();
            slaveGroup = new EpollEventLoopGroup(this.numberOfSlave,
                new BasicThreadFactory.Builder().namingPattern("slave-%d").build());
        } else {

            throw new RuntimeException("Bad mode " + mode);
        }
    }

    public void start() // #1
    {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        });

        try {
            AtomicLong counter = new AtomicLong(0);

            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(masterGroup, slaveGroup);

            if (mode.equalsIgnoreCase("oio")) {
                bootstrap.channel(OioServerSocketChannel.class);
            } else if (mode.equalsIgnoreCase("nio")) {
                bootstrap.channel(NioServerSocketChannel.class);
            } else if (mode.equalsIgnoreCase("nnio")) {
                bootstrap.channel(EpollServerSocketChannel.class);
            }
            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() // #4
            {
                @Override
                public void initChannel(final SocketChannel ch)
                    throws Exception {
                    ch.pipeline().addLast("codec", new HttpServerCodec());
                    ch.pipeline().addLast("aggregator",
                        new HttpObjectAggregator(512 * 1024));
                    ch.pipeline().addLast("request",
                        new ChannelInboundHandlerAdapter() // #5
                        {
                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg)
                                throws Exception {
                                try {
                                    if (msg instanceof FullHttpRequest) {
                                        if (0 == counter.incrementAndGet() % 1000) {
                                            System.out.println(counter.get());
                                        }
                                        final FullHttpRequest request = (FullHttpRequest) msg;

                                        final String responseMessage = "Hello from Netty!";

                                        ByteBuf buf = copiedBuffer(responseMessage.getBytes());
                                        FullHttpResponse response = new DefaultFullHttpResponse(
                                            HttpVersion.HTTP_1_1,
                                            HttpResponseStatus.OK,
                                            buf
                                        );
                                        if (HttpHeaders.isKeepAlive(request)) {
                                            response.headers().set(
                                                HttpHeaders.Names.CONNECTION,
                                                HttpHeaders.Values.KEEP_ALIVE
                                            );
                                        }
                                        response.headers().set(HttpHeaders.Names.CONTENT_TYPE,
                                            "text/plain");
                                        response.headers().set(HttpHeaders.Names.CONTENT_LENGTH,
                                            responseMessage.length());
                                        ctx.writeAndFlush(response)
                                            .addListener(new GenericFutureListener<Future<? super Void>>() {
                                                @Override
                                                public void operationComplete(Future<? super Void> future)
                                                    throws Exception {
                                                    //buf.release();
                                                }
                                            })
                                            .addListener(ChannelFutureListener.CLOSE);
                                    } else {
                                        super.channelRead(ctx, msg);
                                    }
                                } finally {
                                    ((ByteBufHolder) msg).release();
                                }
                            }

                            @Override
                            public void channelReadComplete(ChannelHandlerContext ctx)
                                throws Exception {
                                ctx.flush();
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) throws Exception {
                                ByteBuf buf = copiedBuffer(cause.getMessage().getBytes());
                                ctx.writeAndFlush(new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1,
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                    buf
                                )).addListener(new GenericFutureListener<Future<? super Void>>() {
                                    @Override
                                    public void operationComplete(Future<? super Void> future)
                                        throws Exception {
                                        //buf.release();
                                    }
                                }).addListener(ChannelFutureListener.CLOSE);
                            }
                        });
                }
            })
                .option(ChannelOption.SO_BACKLOG, this.numberOfBacklog)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
            channel = bootstrap.bind(this.port).sync();
        } catch (final InterruptedException e) {
        }
    }

    public void shutdown() // #2
    {
        slaveGroup.shutdownGracefully();
        masterGroup.shutdownGracefully();

        try {
            channel.channel().closeFuture().sync();
        } catch (InterruptedException e) {
        }
    }

    public static void main(String[] args) {

        new NettyHttpServer(args[0],
            args.length > 1 ? args[1] : null,
            args.length > 2 ? args[2] : null,
            args.length > 3 ? args[3] : null).start();
    }
}
