package com.alibaba.otter.canal.deployer.monitor;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 监听instance file的文件变化，触发instance start/stop等操作。
 * InstanceConfigMonitor (配置监控)
 *    Spring模式:
 *      - 监控conf/目录变化
 *      - 每5秒扫描一次
 *      - 新增目录 → 启动实例
 *      - 删除目录 → 停止实例
 *      - 配置变化 → 重载实例
 *
 *    Manager模式:
 *      - 从Admin拉取配置
 *      - 配置变更时重载
 */
public interface InstanceConfigMonitor extends CanalLifeCycle {

    void register(String destination, InstanceAction action);

    void unregister(String destination);
}
