package com.alibaba.otter.canal.instance.spring;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.instance.core.AbstractCanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 基于spring容器启动canal实例，方便独立于manager启动。当我们配置加载方式
 * 为spring时，创建的CanalInstance实例类型都是CanalInstanceWithSpring。
 * canal将会寻找本地的spring配置文件来创建instance实例。
 */
public class CanalInstanceWithSpring extends AbstractCanalInstance {
    private static final Logger logger = LoggerFactory.getLogger(CanalInstanceWithSpring.class);

    @Override
    public void start() {
        logger.info("start CanalInstance for {}-{} ", new Object[] { 1, destination });
        // 调用父类启动Canal实例
        super.start();
    }

    // =====提供了一些set方法为instance的组成模块赋值======

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

    public void setMqConfig(CanalMQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }

}
