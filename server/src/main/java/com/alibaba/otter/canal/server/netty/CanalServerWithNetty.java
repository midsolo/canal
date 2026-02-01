package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * CanalServerWithNetty主要用于接受客户端的请求，然后将其委派给CanalServerWithEmbedded处理。
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {

    // 这是netty监听的网络ip和端口，client通过这个ip和端口与server通信
    private String ip;
    private int port;

    // netty组件
    private Channel serverChannel = null;
    private ServerBootstrap bootstrap = null;
    private ChannelGroup childGroups = null;

    /*
    ==嵌入式服务器，实际处理逻辑==
    因为CanalServerWithNetty需要将请求委派给CanalServerWithEmbedded处理，因此其维护
    了embeddedServer对象监听的所有客户端请求都会为派给CanalServerWithEmbedded处理
     */
    private CanalServerWithEmbedded embeddedServer;

    private CanalServerWithNetty() {
        this.embeddedServer = CanalServerWithEmbedded.instance();
        this.childGroups = new DefaultChannelGroup();
    }

    // 单例模式
    public static CanalServerWithNetty instance() {
        return SingletonHolder.CANAL_SERVER_WITH_NETTY;
    }

    private static class SingletonHolder {
        private static final CanalServerWithNetty CANAL_SERVER_WITH_NETTY = new CanalServerWithNetty();
    }

    @Override
    public void start() {
        super.start();

        // 优先启动内嵌的canal server，因为基于netty的实现需要将请求委派给其处理
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }

        /*
        创建bootstrap实例，参数NioServerSocketChannelFactory也是Netty的API，其接受2个线程池参数，
        其中第一个线程池是Accept线程池，第二个线程池是worker线程池，Accept线程池接收到client连接请
        求后，会将代表client的对象转发给worker线程池处理。
         */
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

        // 配置TCP参数
        bootstrap.setOption("child.keepAlive", true);  // 启用Keep-Alive
        bootstrap.setOption("child.tcpNoDelay", true); // 禁用Nagle算法

        /*
        pipeline实际上就是netty对客户端请求的处理器链，可以类比JAVA EE编程中Filter的
        责任链模式，上一个filter处理完成之后交给下一个filter处理，只不过在netty中，不
        再是filter，而是ChannelHandler
         */
        bootstrap.setPipelineFactory(() -> {
            ChannelPipeline pipelines = Channels.pipeline();

            // 固定头帧解码器（每次），处理网络协议分包，编码，解码。
            // 因为网路传输的传入的都是二进制流，FixedHeaderFrameDecoder的作用就是对其进行解析
            pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());

            // 握手处理器（仅首次），接收客户端的HANDSHAKE请求，处理client与server握手
            pipelines.addLast(HandshakeInitializationHandler.class.getName(), new HandshakeInitializationHandler(childGroups));
            // 认证处理器（仅首次），接收客户端的CLIENTAUTHENTICATION请求，处理client身份验证，会移除HandshakeInitializationHandler和ClientAuthenticationHandler
            pipelines.addLast(ClientAuthenticationHandler.class.getName(), new ClientAuthenticationHandler(embeddedServer));

            /*
            ==会话处理器（每次）==
            处理客户端的所有业务请求
               +-- SUBSCRIPTION    -> subscribe         订阅数据
               +-- UNSUBSCRIPTION  -> unsubscribe       取消订阅
               +-- GET             -> get/getWithoutAck 获取消息
               +-- CLIENTACK       -> ack               确认消息
               +-- CLIENTROLLBACK  -> rollback          回滚消息
             */
            SessionHandler sessionHandler = new SessionHandler(embeddedServer);
            pipelines.addLast(SessionHandler.class.getName(), sessionHandler);

            return pipelines;
        });

        // 启动，当bind方法被调用时，netty开始真正的监控某个端口，此时客户端对这个端口的请求可以被接受到
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    @Override
    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        // close sockets explicitly to reduce socket channel hung in complicated
        // network environment.
        if (this.childGroups != null) {
            this.childGroups.close().awaitUninterruptibly(5000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}
