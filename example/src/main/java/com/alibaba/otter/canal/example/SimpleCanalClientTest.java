package com.alibaba.otter.canal.example;

import java.net.InetSocketAddress;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;

/**
 * 单机模式的测试例子
 */
public class SimpleCanalClientTest extends AbstractCanalClientTest {

    public SimpleCanalClientTest(String destination){
        super(destination);
    }

    public static void main(String args[]) {
        // 配置目标实例名称，对应服务端的某个Canal Instance
        String destination = "example";
        // 获取本机IP
        String ip = AddressUtils.getHostIp();
        // 创建单机模式的连接器
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(ip, 11111), // Canal Server监听的端口11111
                destination,                            // 目标实例名称
                "canal",                                // 用户名
                "canal"                                 // 密码
        );

        // 创建客户端测试实例并启动
        SimpleCanalClientTest clientTest = new SimpleCanalClientTest(destination);
        clientTest.setConnector(connector);
        clientTest.start();
        /*
        ==Canal Client与Canal Server之间是C/S模式的通信，客户端采用NIO，服务端采用Netty==
        当Canal Server启动后，如果没有Canal Client，那么Canal Server不会去MySQL拉取binlog，
        即Canal客户端主动发起拉取请求时，服务端才会模拟一个MySQL Slave节点去主节点拉取binlog，
        通常Canal客户端是一个死循环，这样客户端一直调用#get方法，服务端也就会一直去拉取binlog。
         */

        // 添加JVM关闭钩子，优雅停止
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                clientTest.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }

}
