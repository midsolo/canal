package com.alibaba.otter.canal.parse.inbound;

/**
 * 负责将parse组件解析出的binlog事件(EVENT)传递给sink组件，实现从binlog解析到事件消费的数据流管道。
 * 在Canal的整体架构中，此接口充当parse模块和sink模块之间的桥梁，支持实时的binlog事件处理和转发。
 */
public interface SinkFunction<EVENT> {

    /**
     * 负责将事件数据下沉到下游组件
     *
     * @param event 接收解析后的事件对象
     * @return 返回布尔值表示处理是否成功
     */
    boolean sink(EVENT event);

}