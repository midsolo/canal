package com.alibaba.otter.canal.sink.entry;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.sink.AbstractCanalEventDownStreamHandler;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 使用#before方法过滤HEARTBEAT事件。
 */
public class HeartBeatEntryEventHandler extends AbstractCanalEventDownStreamHandler<List<Event>> {

    @Override
    public List<Event> before(List<Event> events) {
        boolean existHeartBeat = false;
        for (Event event : events) {
            if (event.getEntryType() == EntryType.HEARTBEAT) {
                existHeartBeat = true;
                break;
            }
        }

        if (!existHeartBeat) {
            return events;
        } else {
            // 目前heartbeat和其他事件是分离的，保险一点还是做一下检查处理
            List<Event> result = new ArrayList<>();
            for (Event event : events) {
                if (event.getEntryType() != EntryType.HEARTBEAT) {
                    result.add(event);
                }
            }
            return result;
        }
    }

}
