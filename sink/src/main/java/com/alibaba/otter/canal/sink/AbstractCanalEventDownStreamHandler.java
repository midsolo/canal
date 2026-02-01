package com.alibaba.otter.canal.sink;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;

/**
 * 默认实现方式是什么都不做
 */
public class AbstractCanalEventDownStreamHandler<T>
        extends AbstractCanalLifeCycle implements CanalEventDownStreamHandler<T> {

    public T before(T events) {
        return events;
    }

    public T retry(T events) {
        return events;
    }

    public T after(T events) {
        return events;
    }

}
