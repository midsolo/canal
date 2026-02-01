package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * 在SessionHandler中，会根据请求报文的类型，调用CanalServerWithEmbedded
 * 的相应方法，这些方法都定义在CanalService接口中。
 */
public interface CanalService {

    /*
    每个方法中都包含了一个ClientIdentity类型参数，这就是客户端身份的标识，
    CanalServerWithEmbedded就是根据ClientIdentity中的destination参数确
    定这个请求要交给哪个CanalInstance处理的。
     */

    // 订阅
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;
    // 取消订阅
    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;

    // 比例获取数据，并自动自行ack
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    // 超时时间内批量获取数据，并自动进行ack
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;
    // 批量获取数据，不进行ack
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    // 超时时间内批量获取数据，不进行ack
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;

    // ack某个批次的数据
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;
    // 回滚所有没有ack的批次的数据
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;
    // 回滚某个批次的数据
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;

}
