package com.alibaba.otter.canal.deployer.monitor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * 本地配置动态监控机制：监听基于Spring配置的instance变化。
 * 每5秒扫描一次，如果配置文件有变化，则通知对应的action进行实例操作。
 * 支持：
 *   - 新增实例：自动启动
 *   - 删除实例：自动停止
 *   - 修改实例：自动重载
 * 每5秒扫描 {canal.conf.dir}/{destination}/目录                           │
 * 监控文件: instance.properties
 */
public class SpringInstanceConfigMonitor extends AbstractCanalLifeCycle implements InstanceConfigMonitor, CanalLifeCycle {
    private static final Logger logger = LoggerFactory.getLogger(SpringInstanceConfigMonitor.class);

    private String rootConf;
    private long scanIntervalInSecond = 5; // 扫描周期，单位秒
    private InstanceAction defaultAction = null;
    private Map<String, InstanceAction> actions = new MapMaker().makeMap();
    private Map<String, InstanceConfigFiles> lastFiles = MigrateMap.makeComputingMap(InstanceConfigFiles::new);
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            1, new NamedThreadFactory("canal-instance-scan"));

    private volatile boolean isFirst = true;

    public Map<String, InstanceAction> getActions() {
        return actions;
    }

    @Override
    public void start() {
        super.start();
        Assert.notNull(rootConf, "root conf dir is null!");

        // 启动定时扫描线程，默认每5秒扫描一次
        executor.scheduleWithFixedDelay(() -> {
            try {
                // 扫描配置文件变化
                scan();
                if (isFirst) {
                    isFirst = false;
                }
            } catch (Throwable e) {
                logger.error("scan failed", e);
            }
        }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        executor.shutdownNow();
        actions.clear();
        lastFiles.clear();
    }

    public void register(String destination, InstanceAction action) {
        if (action != null) {
            actions.put(destination, action);
        } else {
            actions.put(destination, defaultAction);
        }
    }

    public void unregister(String destination) {
        actions.remove(destination);
    }

    public void setRootConf(String rootConf) {
        this.rootConf = rootConf;
    }

    private void scan() {
        File rootdir = new File(rootConf);
        if (!rootdir.exists()) {
            return;
        }

        // ========1. 扫描所有实例目录========
        File[] instanceDirs = rootdir.listFiles(pathname -> {
            String filename = pathname.getName();
            // 过滤掉spring目录，只保留实例目录
            return pathname.isDirectory() && !"spring".equalsIgnoreCase(filename);
        });

        // 扫描目录的新增
        Set<String> currentInstanceNames = new HashSet<>();

        // ========2. 遍历每个实例目录========
        for (File instanceDir : instanceDirs) {
            String destination = instanceDir.getName();
            currentInstanceNames.add(destination);
            // 只关注instance.properties文件，避免因为.svn或者其他生成的临时文件导致出现reload
            File[] instanceConfigs = instanceDir.listFiles((dir, name) -> {
                return StringUtils.equalsIgnoreCase(name, "instance.properties");
            });

            if (!actions.containsKey(destination) && instanceConfigs.length > 0) {
                // ========新增实例：启动========
                notifyStart(instanceDir, destination, instanceConfigs);
            } else if (actions.containsKey(destination)) {
                // ========已存在的实例：检查配置是否变化========
                if (instanceConfigs.length == 0) {
                    // 配置文件被删除：停止实例
                    notifyStop(destination);
                } else {
                    InstanceConfigFiles lastFile = lastFiles.get(destination);
                    // 历史启动过 所以配置文件信息必然存在
                    if (!isFirst && CollectionUtils.isEmpty(lastFile.getInstanceFiles())) {
                        logger.error("[{}] is started, but not found instance file info.", destination);
                    }

                    boolean hasChanged = judgeFileChanged(instanceConfigs, lastFile.getInstanceFiles());
                    // 配置文件有变化：重载实例
                    if (hasChanged) {
                        notifyReload(destination);
                    }

                    // 更新文件修改时间记录
                    if (hasChanged || CollectionUtils.isEmpty(lastFile.getInstanceFiles())) {
                        List<FileInfo> newFileInfo = new ArrayList<>();
                        for (File instanceConfig : instanceConfigs) {
                            newFileInfo.add(new FileInfo(instanceConfig.getName(), instanceConfig.lastModified()));
                        }

                        lastFile.setInstanceFiles(newFileInfo);
                    }
                }
            }

        }

        // ========3. 检查是否有实例目录被删除========
        Set<String> deleteInstanceNames = new HashSet<>();
        for (String destination : actions.keySet()) {
            if (!currentInstanceNames.contains(destination)) {
                deleteInstanceNames.add(destination);
            }
        }
        for (String deleteInstanceName : deleteInstanceNames) {
            notifyStop(deleteInstanceName); // 停止已删除的实例
        }
    }

    private void notifyStart(File instanceDir, String destination, File[] instanceConfigs) {
        try {
            defaultAction.start(destination);
            actions.put(destination, defaultAction);

            // 启动成功后记录配置文件信息
            InstanceConfigFiles lastFile = lastFiles.get(destination);
            List<FileInfo> newFileInfo = new ArrayList<>();
            for (File instanceConfig : instanceConfigs) {
                newFileInfo.add(new FileInfo(instanceConfig.getName(), instanceConfig.lastModified()));
            }
            lastFile.setInstanceFiles(newFileInfo);
        } catch (Throwable e) {
            logger.error(String.format("scan add found[%s] but start failed", destination), e);
        }
    }

    private void notifyStop(String destination) {
        InstanceAction action = actions.remove(destination);
        if (action != null) {
            try {
                action.stop(destination);
                lastFiles.remove(destination);
            } catch (Throwable e) {
                logger.error(String.format("scan delete found[%s] but stop failed", destination), e);
                actions.put(destination, action);// 再重新加回去，下一次scan时再执行删除
            }
        }
    }

    private void notifyReload(String destination) {
        InstanceAction action = actions.get(destination);
        if (action != null) {
            try {
                action.reload(destination);
            } catch (Throwable e) {
                logger.error(String.format("scan reload found[%s] but reload failed", destination), e);
            }
        }
    }

    private boolean judgeFileChanged(File[] instanceConfigs, List<FileInfo> fileInfos) {
        boolean hasChanged = false;
        for (File instanceConfig : instanceConfigs) {
            for (FileInfo fileInfo : fileInfos) {
                if (instanceConfig.getName().equals(fileInfo.getName())) {
                    hasChanged |= (instanceConfig.lastModified() != fileInfo.getLastModified());
                    if (hasChanged) {
                        return hasChanged;
                    }
                }
            }
        }

        return hasChanged;
    }

    public void setDefaultAction(InstanceAction defaultAction) {
        this.defaultAction = defaultAction;
    }

    public void setScanIntervalInSecond(long scanIntervalInSecond) {
        this.scanIntervalInSecond = scanIntervalInSecond;
    }

    public static class InstanceConfigFiles {

        private String destination;                              // instance
        // name
        private List<FileInfo> springFile = new ArrayList<>(); // spring的instance
        // xml
        private FileInfo rootFile;                                 // canal.properties
        private List<FileInfo> instanceFiles = new ArrayList<>(); // instance对应的配置

        public InstanceConfigFiles(String destination) {
            this.destination = destination;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public List<FileInfo> getSpringFile() {
            return springFile;
        }

        public void setSpringFile(List<FileInfo> springFile) {
            this.springFile = springFile;
        }

        public FileInfo getRootFile() {
            return rootFile;
        }

        public void setRootFile(FileInfo rootFile) {
            this.rootFile = rootFile;
        }

        public List<FileInfo> getInstanceFiles() {
            return instanceFiles;
        }

        public void setInstanceFiles(List<FileInfo> instanceFiles) {
            this.instanceFiles = instanceFiles;
        }

    }

    public static class FileInfo {

        private String name;
        private long lastModified = 0;

        public FileInfo(String name, long lastModified) {
            this.name = name;
            this.lastModified = lastModified;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getLastModified() {
            return lastModified;
        }

        public void setLastModified(long lastModified) {
            this.lastModified = lastModified;
        }

    }
}
