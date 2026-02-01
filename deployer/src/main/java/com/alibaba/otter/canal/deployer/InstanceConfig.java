package com.alibaba.otter.canal.deployer;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 启动的相关配置
 */
public class InstanceConfig {

    private InstanceConfig globalConfig;  // 全局配置（父配置）
    private InstanceMode   mode;          // 实例模式：SPRING/MANAGER
    private Boolean        lazy;          // 是否延迟启动
    private String         managerAddress;// Manager地址
    private String         springXml;     // Spring配置文件路径
    // ======== 配置继承机制 ========
    // 如果当前实例没有配置某个属性，则从 globalConfig 中获取

    public InstanceConfig(){

    }

    public InstanceConfig(InstanceConfig globalConfig){
        this.globalConfig = globalConfig;
    }

    public static enum InstanceMode {
        SPRING, MANAGER;

        public boolean isSpring() {
            return this == InstanceMode.SPRING;
        }

        public boolean isManager() {
            return this == InstanceMode.MANAGER;
        }
    }

    public Boolean getLazy() {
        if (lazy == null && globalConfig != null) {
            return globalConfig.getLazy(); // 使用全局配置
        } else {
            return lazy;                   // 使用实例配置
        }
    }

    public void setLazy(Boolean lazy) {
        this.lazy = lazy;
    }

    public InstanceMode getMode() {
        if (mode == null && globalConfig != null) {
            return globalConfig.getMode();
        } else {
            return mode;
        }
    }

    public void setMode(InstanceMode mode) {
        this.mode = mode;
    }

    public String getManagerAddress() {
        if (managerAddress == null && globalConfig != null) {
            return globalConfig.getManagerAddress();
        } else {
            return managerAddress;
        }
    }

    public void setManagerAddress(String managerAddress) {
        this.managerAddress = managerAddress;
    }

    public String getSpringXml() {
        if (springXml == null && globalConfig != null) {
            return globalConfig.getSpringXml();
        } else {
            return springXml;
        }
    }

    public void setSpringXml(String springXml) {
        this.springXml = springXml;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
