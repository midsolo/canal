package com.alibaba.otter.canal.store;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.model.Events;

/**
 * Canal数据存储接口。
 * store模块用于binlog事件的存储 ，目前开源的版本中仅实现了Memory内存模式。官方文档
 * 中提到"后续计划增加本地file存储，mixed混合模式”，这句话大家不必当真，从笔者最开始
 * 接触Canal到现在已经几年了，依然没动静，好在Memory内存模式已经可以满足绝大部分场景。
 */
public interface CanalEventStore<T> extends CanalLifeCycle, CanalStoreScavenge {

    // ==========================Put操作==============================
    // 添加一组数据对象，阻塞等待其操作完成 (比如一次性添加一个事务数据)
    void put(List<T> data) throws InterruptedException, CanalStoreException;
    //添加一组数据对象，阻塞等待其操作完成或者时间超时 (比如一次性添加一个事务数据)
    boolean put(List<T> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;
    // 添加一组数据对象 (比如一次性添加一个事务数据)
    boolean tryPut(List<T> data) throws CanalStoreException;
    // 添加一个数据对象，阻塞等待其操作完成
    void put(T data) throws InterruptedException, CanalStoreException;
    // 添加一个数据对象，阻塞等待其操作完成或者时间超时
    boolean put(T data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;
    // 添加一个数据对象
    boolean tryPut(T data) throws CanalStoreException;

    // ==========================GET操作==============================
    // 获取指定大小的数据，阻塞等待其操作完成
    Events<T> get(Position start, int batchSize) throws InterruptedException, CanalStoreException;
    // 获取指定大小的数据，阻塞等待其操作完成或者时间超时
    Events<T> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;
    // 根据指定位置，获取一个指定大小的数据
    Events<T> tryGet(Position start, int batchSize) throws CanalStoreException;
    // 获取最后一条数据的position
    Position getLatestPosition() throws CanalStoreException;
    // 获取第一条数据的position，如果没有数据返回为null
    Position getFirstPosition() throws CanalStoreException;

    // =========================Ack操作==============================
    // 删除{@linkplain Position}之前的数据
    void ack(Position position) throws CanalStoreException;
    // 删除指定seqId之前的数据
    void ack(Position position, Long seqId) throws CanalStoreException;
    // 出错时执行回滚操作(未提交ack的所有状态信息重新归位，减少出错时数据全部重来的成本)
    void rollback() throws CanalStoreException;

}
