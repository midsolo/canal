package com.alibaba.otter.canal.client.impl.running;

/**
 * 客户端运行状态
 */
public class ClientRunningData {

    private short clientId;
    private String address;
    private boolean active = true;

    public short getClientId() {
        return clientId;
    }

    public void setClientId(short clientId) {
        this.clientId = clientId;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

}
