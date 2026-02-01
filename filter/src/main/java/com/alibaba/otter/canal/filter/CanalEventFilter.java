package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;

/**
 * ==filter数据过滤组件==
 * filter组件用于对binlog进行过滤。在实际开发中，一个mysql实例中可能会有多个库，每个库里面又会有多个表，
 * 可能我们只是想订阅某个库中的部分表，这个时候就需要进行过滤。也就是说，parser模块解析出来binlog之后，
 * 会进行一次过滤之后，才会存储到store模块中。过滤规则的配置既可以在canal服务端进行，也可以在客户端进行。
 */
public interface CanalEventFilter<T> {

    boolean filter(T event) throws CanalFilterException;

}