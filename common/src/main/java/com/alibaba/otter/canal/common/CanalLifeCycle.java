package com.alibaba.otter.canal.common;

/**
 * Canal生命周期
 */
public interface CanalLifeCycle {

    void start();
    void stop();
    boolean isStart();

}
