package com.alibaba.otter.canal.instance.core;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * AbstractCanalInstance是CanalInstance的抽象子类，定义了相关字段来维护eventParser、eventSink、
 * eventStore、metaManager的引用。需要注意的是，在AbstractCanalInstance中，并没有提供方法来初始
 * 化这些字段。可以看到，这些字段都是protected的，子类可以直接访问，显然这些字段都是在AbstractCanalInstance
 * 的子类中进行赋值的。AbstractCanalInstance不关心这些字段的具体实现，只是从接口层面进行调用。对于
 * 其子类而言，只需要给相应的字段赋值即可。
 * 因此，对于instance模块而言，其核心工作逻辑都是在AbstractCanalInstance中实现的。
 */
public class AbstractCanalInstance extends AbstractCanalLifeCycle implements CanalInstance {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCanalInstance.class);

    protected Long canalId;                                     // 和manager交互唯一标识
    protected String destination;                               // 队列名字
    protected CanalEventParser eventParser;                     // 解析对应的数据信息
    protected CanalEventSink<List<CanalEntry.Entry>> eventSink; // 链接parse和store的桥接器
    protected CanalEventStore<Event> eventStore;                // 有序队列
    protected CanalMetaManager metaManager;                     // 消费信息管理器
    protected CanalAlarmHandler alarmHandler;                   // alarm报警机制
    protected CanalMQConfig mqConfig;                           // mq的配置

    @Override // 启动各个模块，启动顺序为：metaManager—>eventStore—>eventSink—>eventParser
    public void start() {
        super.start();

        // metaManager是最基础的部分，因此应该最先启动
        if (!metaManager.isStart()) {
            metaManager.start();
        }
        if (!alarmHandler.isStart()) {
            alarmHandler.start();
        }

        /*
        eventStore是数据存储，必须先启动
        eventParser依赖于eventSink，需要把自己解析的binlog交给其加工过滤，
        而eventSink又要把处理后的数据交给eventStore进行存储。因此依赖关系
        如下：eventStore—>eventSink—>eventParser ，启动的时候也要按照这
        个顺序启动。
         */
        if (!eventStore.isStart()) {
            eventStore.start();
        }
        // eventSink依赖eventStore
        if (!eventSink.isStart()) {
            eventSink.start();
        }
        // eventParser依赖eventSink
        if (!eventParser.isStart()) {
            // 启动logPositionManager和haController：eventParser在启动之前，需要先启动CanalLogPositionManager和CanalHAController
            beforeStartEventParser(eventParser);
            eventParser.start();
            // 加载历史filter信息：通过metaManager读取一下历史订阅过这个CanalInstance的客户端信息，然后更新一下filter
            afterStartEventParser(eventParser);
        }

        logger.info("start successful....");
    }

    @Override // 停止内部的各个模块，模块停止的顺序与start方法刚好相反
    public void stop() {
        super.stop();
        logger.info("stop CannalInstance for {}-{} ", new Object[]{canalId, destination});
        if (eventParser.isStart()) {
            beforeStopEventParser(eventParser);
            eventParser.stop();
            afterStopEventParser(eventParser);
        }
        if (eventSink.isStart()) {
            eventSink.stop();
        }
        if (eventStore.isStart()) {
            eventStore.stop();
        }
        if (metaManager.isStart()) {
            metaManager.stop();
        }
        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }
        logger.info("stop successful....");
    }

    @Override // 更新一下eventParser中的filter
    public boolean subscribeChange(ClientIdentity identity) {
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            logger.info("subscribe filter change to " + identity.getFilter());
            // filter规定了需要订阅哪些库，哪些表。在服务端和客户端都可以设置，客户端的配置会覆盖服务端的配置
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());
            boolean isGroup = (eventParser instanceof GroupEventParser);
            // 处理group的模式
            if (isGroup) {
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                    if (singleEventParser instanceof AbstractEventParser) {
                        ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                    }
                }
            } else {
                if (eventParser instanceof AbstractEventParser) {
                    ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
                }
            }
        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    protected void beforeStartEventParser(CanalEventParser eventParser) {
        // 判断eventParser的类型是否是GroupEventParser
        boolean isGroup = (eventParser instanceof GroupEventParser);
        // 如果是GroupEventParser，则循环启动其内部包含的每一个CanalEventParser，依次调用startEventParserInternal方法
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            // 需要遍历启动
            for (CanalEventParser singleEventParser : eventParsers) {
                startEventParserInternal(singleEventParser, true);
            }
        }
        // 如果不是，说明是一个普通的CanalEventParser，直接调用startEventParserInternal方法
        else {
            // 其内部会启动CanalLogPositionManager和CanalHAController
            startEventParserInternal(eventParser, false);
        }
    }

    protected void afterStartEventParser(CanalEventParser eventParser) {
        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            // 更新filter
            subscribeChange(clientIdentity);
        }
    }

    // around event parser
    protected void beforeStopEventParser(CanalEventParser eventParser) {
        // noop
    }

    protected void afterStopEventParser(CanalEventParser eventParser) {
        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    /**
     * 初始化单个eventParser，不需要考虑group
     */
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        // 启动CanalLogPositionManager
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            /*
            mysql在主从同步过程中，要求slave自己维护binlog的消费进度信息。canal伪装成slave，因此也要维护
            这样的信息。事实上，如果你自己有搭建过mysql主从复制的话，在slave机器的data目录下，都会有一个
            master.info文件，这个文件的作用就是存储主库的消费binlog解析进度信息。
             */
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (!logPositionManager.isStart()) {
                logPositionManager.start();
            }
        }

        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();

            if (haController instanceof HeartBeatHAController) {
                ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
            }

            if (!haController.isStart()) {
                haController.start();
            }
        }
    }

    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (logPositionManager.isStart()) {
                logPositionManager.stop();
            }
        }
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController.isStart()) {
                haController.stop();
            }
        }
    }

    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public CanalEventParser getEventParser() {
        return eventParser;
    }

    @Override
    public CanalEventSink getEventSink() {
        return eventSink;
    }

    @Override
    public CanalEventStore getEventStore() {
        return eventStore;
    }

    @Override
    public CanalMetaManager getMetaManager() {
        return metaManager;
    }

    @Override
    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    @Override
    public CanalMQConfig getMqConfig() {
        return mqConfig;
    }
}
