package com.alibaba.otter.canal.instance.core;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;

/**
 * ==CanalInstance代表单个canal实例，一个destination会对应一个实例==
 * 每个CanalInstance中包括了四个组件：EventParser、EventSink、EventStore、MetaManager。
 * instance模块其实是把这几个模块组装在一起，为客户端的binlog订阅请求提供服务。有些模块都
 * 有多种实现，不同组合方式，最终确定了一个CanalInstance的工作逻辑。服务端主要的处理方法包
 * 括get/ack/rollback，这三个方法都会用到Instance上面的几个内部组件，主要还是EventStore
 * 和MetaManager。
 */
public interface CanalInstance extends CanalLifeCycle {

    String getDestination();            // 这个instance对应的destination

    CanalEventParser getEventParser();  // 解析器-从MySQL拉取binlog   | 数据源接入，模拟slave协议和master进行交互，协议解析
    CanalEventSink   getEventSink();    // 处理器-过滤/分发binlog事件 | parser和store链接器，进行数据过滤，加工，分发的工作
    CanalEventStore  getEventStore();   // 存储器-缓存binlog事件      | 数据存储
    CanalMetaManager getMetaManager();  // 元数据管理-管理消费位置     | 增量订阅/消费binlog元数据位置存储

    CanalAlarmHandler getAlarmHandler();// 告警，位于canal.common块中

    boolean subscribeChange(ClientIdentity identity); // 客户端发生订阅/取消订阅行为

    CanalMQConfig getMqConfig();

}
