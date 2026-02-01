package com.alibaba.otter.canal.sink;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.filter.CanalEventFilter;

/**
 * 该抽象类组合了CanalEventFilter和CanalEventDownStreamHandler的能力。
 */
public abstract class AbstractCanalEventSink<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

    // 管理CanalEventFilter
    protected CanalEventFilter filter;
    // 通过#setFilter方法，可以将一个具体的CanalEventFilter实现类注入到Sink组件中
    public void setFilter(CanalEventFilter filter) {
        this.filter = filter;
    }
    public CanalEventFilter getFilter() {
        return filter;
    }

    // 管理CanalEventDownStreamHandler
    protected List<CanalEventDownStreamHandler> handlers = new ArrayList<>();
    public void addHandler(CanalEventDownStreamHandler handler) {
        this.handlers.add(handler);
    }
    public void addHandler(CanalEventDownStreamHandler handler, int index) {
        this.handlers.add(index, handler);
    }
    public List<CanalEventDownStreamHandler> getHandlers() {
        return handlers;
    }
    public CanalEventDownStreamHandler getHandler(int index) {
        return this.handlers.get(index);
    }
    public void removeHandler(int index) {
        this.handlers.remove(index);
    }
    public void removeHandler(CanalEventDownStreamHandler handler) {
        this.handlers.remove(handler);
    }

    // 可中断消费，由子类实现
    public void interrupt() { }

}
