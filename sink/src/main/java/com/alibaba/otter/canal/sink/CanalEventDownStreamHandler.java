package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 定义CanalSink中事件处理的回调点，这些回调允许在事件提交到store组件存储之前、
 * 成功存储之后或存储已满重试期间执行自定义逻辑。这种机制允许Canal Sink中进行
 * 灵活可扩展的事件处理，使开发人员能够在事件生命周期的关键点注入自定义逻辑。
 */
public interface CanalEventDownStreamHandler<T> extends CanalLifeCycle {

    /**
     * 在事件提交到CanalEventStore之前被调用，允许对事件进行预处理或修改。
     * 例如：HeartBeatEntryEventHandler使用before方法过滤HEARTBEAT事件。
     */
    T before(T events);

    /**
     * 当CanalEventStore已满且put操作失败时被调用，允许对事件进行重试处理，
     * 它用于处理事件无法立即存储的场景。
     */
    T retry(T events);

    /**
     * 在事件成功提交到CanalEventStore之后被调用，它可用于后置处理或通知。
     */
    T after(T events);

}
