package com.alibaba.otter.canal.parse.inbound;

import java.io.IOException;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;

/**
 * 通用的Erosa的链接接口, 用于一般化处理mysql/oracle的解析过程
 */
public interface ErosaConnection {

    void connect() throws IOException;
    void reconnect() throws IOException;
    void disconnect() throws IOException;

    // 用于快速数据查找，和dump的区别在于，seek会只给出部分的数据
    void seek(String binlogfilename, Long binlogPosition, String gtid, SinkFunction func) throws IOException;

    void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException;
    void dump(long timestamp, SinkFunction func) throws IOException;
    // 通过GTID同步binlog
    void dump(GTIDSet gtidSet, SinkFunction func) throws IOException;
    void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException;
    void dump(long timestamp, MultiStageCoprocessor coprocessor) throws IOException;
    void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException;

    ErosaConnection fork();

    long queryServerId() throws IOException;
}
