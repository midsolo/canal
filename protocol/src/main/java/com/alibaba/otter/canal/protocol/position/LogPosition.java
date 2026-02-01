package com.alibaba.otter.canal.protocol.position;

/**
 * 完整的位置信息
 */
public class LogPosition extends Position {
    private static final long serialVersionUID = 3875012010277005819L;

    private LogIdentity       identity; // 数据来源标识（地址、slaveId）
    private EntryPosition     postion;  // 具体的binlog位置

    public LogIdentity getIdentity() {
        return identity;
    }

    public void setIdentity(LogIdentity identity) {
        this.identity = identity;
    }

    public EntryPosition getPostion() {
        return postion;
    }

    public void setPostion(EntryPosition postion) {
        this.postion = postion;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identity == null) ? 0 : identity.hashCode());
        result = prime * result + ((postion == null) ? 0 : postion.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof LogPosition)) {
            return false;
        }
        LogPosition other = (LogPosition) obj;
        if (identity == null) {
            if (other.identity != null) {
                return false;
            }
        } else if (!identity.equals(other.identity)) {
            return false;
        }
        if (postion == null) {
            if (other.postion != null) {
                return false;
            }
        } else if (!postion.equals(other.postion)) {
            return false;
        }
        return true;
    }

}
