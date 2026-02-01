package com.alibaba.otter.canal.example;

import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 测试基类
 */
public class AbstractCanalClientTest extends BaseCanalClientTest {

    public AbstractCanalClientTest(String destination){
        this(destination, null);
    }

    public AbstractCanalClientTest(String destination, CanalConnector connector){
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        // 启动处理线程
        thread.start();
    }

    /**
     * 主处理循环
     *  AbstractCanalClientTest.process()主处理循环
     *           |
     *           +--> connector.connect()       建立连接
     *           +--> connector.subscribe()     订阅数据
     *           +--> connector.getWithoutAck() 获取消息（循环）
     *           +--> connector.ack()           确认消费
     *           +--> connector.rollback()      失败回滚
     *           +--> connector.disconnect()    确认消费
     */
    protected void process() {
        int batchSize = 5 * 1024; // 每次拉取最多5120条记录
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();  // 连接服务端
                connector.subscribe();// 订阅（可传过滤条件，如 "test.table"）
                while (running) {
                    /*
                    ==获取消息（不自动确认）==
                    允许指定batchSize，一次可以获取多条，每次返回的对象为Message，包含的内容为：
                        – batch id 唯一标识
                        – entries 具体的数据对象
                    对应的数据对象格式：EntryProtocol.proto
                     */
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // 没有数据，继续轮询
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                        printSummary(message, batchId, size); // 打印摘要
                        printEntry(message.getEntries());     // 打印详情
                    }

                    // 确认消费成功，删除服务端数据
                    if (batchId != -1) {
                        // 确认已经消费成功，通知server删除数据。基于get获取的batchId进行提交，避免误操作
                        connector.ack(batchId);
                    }
                }
            } catch (Throwable e) {
                logger.error("process error!", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }

                /*
                异常时回滚，重新消费
                回滚上次的get请求，重新获取数据。基于get获取的batchId进行提交，避免误操作
                 */
                connector.rollback();
            } finally {
                // 断开连接
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }
}
