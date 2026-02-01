package com.alibaba.otter.canal.store.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;
import com.alibaba.otter.canal.protocol.position.PositionRange;

/**
 * 代表一组数据对象的集合
 */
public class Events<EVENT> implements Serializable {
    private static final long serialVersionUID = -7337454954300706044L;

    // 描述了这组Event的开始(start)和结束位置(end)，start表示List集合中第一个Event的位置，end表示最后一个Event的位置
    private PositionRange positionRange = new PositionRange();
    // 通过一个List维护了一组数据，尽管这里定义的是EVENT泛型，但真实放入的数据实际上是Event类型
    private List<EVENT> events = new ArrayList<>();

    public List<EVENT> getEvents() {
        return events;
    }

    public void setEvents(List<EVENT> events) {
        this.events = events;
    }

    public PositionRange getPositionRange() {
        return positionRange;
    }

    public void setPositionRange(PositionRange positionRange) {
        this.positionRange = positionRange;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
