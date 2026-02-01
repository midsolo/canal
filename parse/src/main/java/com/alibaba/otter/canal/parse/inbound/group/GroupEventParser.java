package com.alibaba.otter.canal.parse.inbound.group;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.CanalEventParser;

/**
 * 伪装成多个mysql实例的slave解析binlog日志。内部维护了多个CanalEventParser。主要应用场景
 * 是分库分表：比如产品数据拆分了4个库，位于不同的mysql实例上。正常情况下，我们需要配置四个
 * CanalInstance。对应的，业务上要消费数据时，需要启动4个客户端，分别链接4个instance实例。
 * 为了方便业务使用，此时我们可以让CanalInstance引用一个GroupEventParser，由GroupEventParser
 * 内部维护4个MysqlEventParser去4个不同的mysql实例去拉取binlog，最终合并到一起。此时业务只
 * 需要启动1个客户端，链接这个CanalInstance即可.
 */
public class GroupEventParser extends AbstractCanalLifeCycle implements CanalEventParser {

    // 内部维护了多个CanalEventParser
    private List<CanalEventParser> eventParsers = new ArrayList<>();

    @Override
    public void start() {
        super.start();
        // 多个CanalEventParser统一启动
        for (CanalEventParser eventParser : eventParsers) {
            if (!eventParser.isStart()) {
                eventParser.start();
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        // 多个CanalEventParser统一关闭
        for (CanalEventParser eventParser : eventParsers) {
            if (eventParser.isStart()) {
                eventParser.stop();
            }
        }
    }

    public void setEventParsers(List<CanalEventParser> eventParsers) {
        this.eventParsers = eventParsers;
    }

    public void addEventParser(CanalEventParser eventParser) {
        if (!eventParsers.contains(eventParser)) {
            eventParsers.add(eventParser);
        }
    }

    public void removeEventParser(CanalEventParser eventParser) {
        eventParsers.remove(eventParser);
    }

    public List<CanalEventParser> getEventParsers() {
        return eventParsers;
    }

}
