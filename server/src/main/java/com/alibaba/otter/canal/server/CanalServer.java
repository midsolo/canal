package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.server.exception.CanalServerException;

/**
 * CanalServer接口继承了CanalLifeCycle接口，主要是为了重新定义start和stop方法，
 * 抛出CanalServerException。对应canal整个服务实例，一个jvm实例只有一份server。
 */
public interface CanalServer extends CanalLifeCycle {

    void start() throws CanalServerException;
    void stop() throws CanalServerException;

}
