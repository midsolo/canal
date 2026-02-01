package com.alibaba.otter.canal.common.alarm;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * canal报警处理机制
 */
public interface CanalAlarmHandler extends CanalLifeCycle {

    /**
     * 发送对应destination的报警
     * 
     * @param destination
     * @param msg
     */
    void sendAlarm(String destination, String msg);
}
