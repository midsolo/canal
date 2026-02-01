package com.alibaba.otter.canal.deployer;

import static com.alibaba.otter.canal.deployer.CanalConstants.CANAL_DESTINATIONS;
import static com.alibaba.otter.canal.deployer.CanalConstants.CANAL_DESTINATIONS_EXPR;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningListener;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.ManagerInstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.PlainCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;
import com.alibaba.otter.canal.instance.spring.SpringCanalInstanceGenerator;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * CanalServer真正的启动控制器
 */
public class CanalController {
    private static final Logger logger = LoggerFactory.getLogger(CanalController.class);

    private String ip;
    private String registerIp;
    private int port;
    private int adminPort;
    // 默认使用spring的方式载入
    private Map<String, InstanceConfig> instanceConfigs;
    private InstanceConfig globalInstanceConfig;
    private Map<String, PlainCanalConfigClient> managerClients;
    // 监听instance config的变化
    private boolean autoScan = true;
    private InstanceAction defaultAction;
    private Map<InstanceMode, InstanceConfigMonitor> instanceConfigMonitors;

    // CanalServer支持两种模式，CanalServerWithEmbedded和CanalServerWithNetty
    private CanalServerWithEmbedded embeddedCanalServer;
    private CanalServerWithNetty canalServer;

    private CanalInstanceGenerator instanceGenerator;
    private ZkClientx zkclientx;

    private CanalMQStarter canalMQStarter;
    private String adminUser;
    private String adminPasswd;

    public CanalController() {
        this(System.getProperties());
    }

    public CanalController(final Properties properties) {
        /*
        ========初始化配置客户端（用于Manager模式）========
        单机版不走这里，但保留兼容性，managerClients用于从Admin远程拉取配置
         */
        managerClients = MigrateMap.makeComputingMap(this::getManagerClient);

        /*
        ========1. 初始化全局配置========
        读取全局配置，作为所有Instance的默认配置，包括：mode、lazy、springXml等
         */
        globalInstanceConfig = initGlobalConfig(properties);

        /*
        ========2. 初始化实例配置========
        读取canal.destinations，为每个实例创建配置
        支持：
          - 普通列表: example,order,payment
          - 表达式: order{1-3} → order1,order2,order3
         */
        instanceConfigs = new MapMaker().makeMap();
        initInstanceConfig(properties);

        // init socketChannel
        String socketChannel = getProperty(properties, CanalConstants.CANAL_SOCKETCHANNEL);
        if (StringUtils.isNotEmpty(socketChannel)) {
            System.setProperty(CanalConstants.CANAL_SOCKETCHANNEL, socketChannel);
        }

        // 兼容1.1.0版本的ak/sk参数名
        String accesskey = getProperty(properties, "canal.instance.rds.accesskey");
        String secretkey = getProperty(properties, "canal.instance.rds.secretkey");
        if (StringUtils.isNotEmpty(accesskey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_ACCESSKEY, accesskey);
        }
        if (StringUtils.isNotEmpty(secretkey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_SECRETKEY, secretkey);
        }

        // ========3. 读取网络配置========
        ip = getProperty(properties, CanalConstants.CANAL_IP);
        registerIp = getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
        port = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_PORT, "11111"));
        adminPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110"));

        /*
        ========4. 初始化CanalServerWithEmbedded========
        CanalServerWithEmbedded是核心业务处理服务器，负责管理所有CanalInstance
         */
        embeddedCanalServer = CanalServerWithEmbedded.instance(); // 单例模式
        /*
        ======关键：设置InstanceGenerator======
        InstanceGenerator负责创建CanalInstance，当客户端请求某个destination时，通过这个Generator创建对应的Instance
         */
        embeddedCanalServer.setCanalInstanceGenerator(instanceGenerator);

        // 设置metrics端口
        int metricsPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_METRICS_PULL_PORT, "11112"));
        embeddedCanalServer.setMetricsPort(metricsPort);

        this.adminUser = getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
        this.adminPasswd = getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);

        // 设置用户认证信息（TCP模式下的客户端认证）
        String user = getProperty(properties, CanalConstants.CANAL_USER);
        String passwd = getProperty(properties, CanalConstants.CANAL_PASSWD);
        if (StringUtils.isNotEmpty(user) && StringUtils.isEmpty(passwd)) {
            throw new IllegalArgumentException("canal.user = " + user + " , but canal.passwd is empty , pls check https://github.com/alibaba/canal/issues/4941");
        }
        embeddedCanalServer.setUser(user);
        embeddedCanalServer.setPasswd(passwd);

        // ========5. 初始化CanalServerWithNetty（如果启用）========
        String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
        if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
            canalServer = CanalServerWithNetty.instance(); // 单例模式，TCP模式需要Netty
            canalServer.setIp(ip);
            canalServer.setPort(port);
        }

        // 处理下ip为空，默认使用hostIp暴露到zk中
        if (StringUtils.isEmpty(ip) && StringUtils.isEmpty(registerIp)) {
            ip = registerIp = AddressUtils.getHostIp();
        }

        if (StringUtils.isEmpty(ip)) {
            ip = AddressUtils.getHostIp();
        }

        if (StringUtils.isEmpty(registerIp)) {
            registerIp = ip; // 兼容以前配置
        }

        /*
        ========6. 初始化 ZooKeeper（如果配置了）========
        单机版通常不配置ZooKeeper，如果配置了，走HA模式
         */
        final String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
        if (StringUtils.isNotEmpty(zkServers)) {
            zkclientx = ZkClientx.getZkClient(zkServers);
            // 初始化系统目录
            zkclientx.createPersistent(ZookeeperPathUtils.DESTINATION_ROOT_NODE, true);
            zkclientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true);
        }

        // ========7. 初始化ServerRunningMonitor（HA机制）========
        final ServerRunningData serverData = new ServerRunningData(registerIp + ":" + port);
        /*
        为每个destination创建RunningMonitor
           - 有ZK：通过ZK抢占锁，只有Active节点运行Instance
           - 无ZK：直接运行Instance（单机版走这里）
         */
        ServerRunningMonitors.setServerData(serverData);
        // 为每个destination创建一个RunningMonitor
        ServerRunningMonitors.setRunningMonitors(MigrateMap.makeComputingMap((Function<String, ServerRunningMonitor>) destination -> {
            ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
            runningMonitor.setDestination(destination);
            // 设置运行状态监听器
            runningMonitor.setListener(new ServerRunningListener() {
                @Override // 成为Active时调用
                public void processActiveEnter() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        // 启动CanalInstance
                        embeddedCanalServer.start(destination);
                        if (canalMQStarter != null) {
                            canalMQStarter.startDestination(destination);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                @Override // 退出 Active 时调用
                public void processActiveExit() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (canalMQStarter != null) {
                            canalMQStarter.stopDestination(destination);
                        }
                        embeddedCanalServer.stop(destination);
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                @Override
                public void processStart() {
                    try {
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, registerIp + ":" + port);
                            initCid(path);
                            zkclientx.subscribeStateChanges(new IZkStateListener() {
                                @Override
                                public void handleStateChanged(KeeperState state) throws Exception {

                                }
                                @Override
                                public void handleNewSession() throws Exception {
                                    initCid(path);
                                }
                                @Override
                                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                                    logger.error("failed to connect to zookeeper", error);
                                }
                            });
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }

                @Override
                public void processStop() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, registerIp + ":" + port);
                            releaseCid(path);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
            });

            if (zkclientx != null) {
                runningMonitor.setZkClient(zkclientx);
            }

            // 触发创建一下cid节点
            runningMonitor.init();
            return runningMonitor;
        }));

        // ========8. 初始化配置监控机制========
        autoScan = BooleanUtils.toBoolean(getProperty(properties, CanalConstants.CANAL_AUTO_SCAN));
        if (autoScan) {
            // 定义默认的InstanceAction（启动/停止/重载/释放操作）
            defaultAction = new InstanceAction() {
                @Override
                public void start(String destination) {
                    // 获取或解析实例配置
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config == null) {
                        // 重新读取一下instance config
                        config = parseInstanceConfig(properties, destination);
                        instanceConfigs.put(destination, config);
                    }

                    // 启动Instance（通过HA机制）
                    if (!embeddedCanalServer.isStart(destination)) {
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (!config.getLazy() && !runningMonitor.isStart()) {
                            // 触发HA抢占
                            runningMonitor.start();
                        }
                    }

                    logger.info("auto notify start {} successful.", destination);
                }

                @Override
                public void stop(String destination) {
                    // 此处的stop，代表强制退出，非HA机制，所以需要退出HA的monitor和配置信息
                    InstanceConfig config = instanceConfigs.remove(destination);
                    if (config != null) {
                        embeddedCanalServer.stop(destination);
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            runningMonitor.stop();
                        }
                    }

                    logger.info("auto notify stop {} successful.", destination);
                }

                @Override // 重载=先停止，再启动
                public void reload(String destination) {
                    // 目前任何配置变化，直接重启，简单处理
                    stop(destination);
                    start(destination);
                    logger.info("auto notify reload {} successful.", destination);
                }

                @Override
                public void release(String destination) {
                    // 释放HA运行状态，让其他机器抢占
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config != null) {
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            boolean release = runningMonitor.release();
                            if (!release) {
                                // 单机模式，直接清除配置并停止
                                instanceConfigs.remove(destination);
                                runningMonitor.stop();
                                if (instanceConfigMonitors.containsKey(InstanceConfig.InstanceMode.MANAGER)) {
                                    ManagerInstanceConfigMonitor monitor = (ManagerInstanceConfigMonitor) instanceConfigMonitors.get(InstanceConfig.InstanceMode.MANAGER);
                                    Map<String, InstanceAction> instanceActions = monitor.getActions();
                                    if (instanceActions.containsKey(destination)) {
                                        // 清除内存中的autoScan cache
                                        monitor.release(destination);
                                    }
                                }
                            }
                        }
                    }

                    logger.info("auto notify release {} successful.", destination);
                }
            };

            // 创建不同模式的配置监控器
            instanceConfigMonitors = MigrateMap.makeComputingMap(mode -> {
                int scanInterval = Integer.valueOf(getProperty(properties,
                        CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                if (mode.isSpring()) {
                    // Spring模式：监控本地配置文件变化（单机版走这里）
                    SpringInstanceConfigMonitor monitor = new SpringInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval); // 默认5秒
                    monitor.setDefaultAction(defaultAction);
                    // 设置conf目录，默认是user.dir + conf目录组成
                    String rootDir = getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                    if (StringUtils.isEmpty(rootDir)) {
                        rootDir = "../conf"; // 默认配置目录
                    }

                    if (StringUtils.equals("otter-canal", System.getProperty("appName"))) {
                        monitor.setRootConf(rootDir);
                    } else {
                        // eclipse debug模式
                        monitor.setRootConf("src/main/resources/");
                    }
                    return monitor;
                } else if (mode.isManager()) {
                    // Manager模式：监控Admin远程配置变化
                    ManagerInstanceConfigMonitor monitor = new ManagerInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval);
                    monitor.setDefaultAction(defaultAction);
                    String managerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
                    monitor.setConfigClient(getManagerClient(managerAddress));
                    return monitor;
                } else {
                    throw new UnsupportedOperationException("unknow mode :" + mode + " for monitor");
                }
            });
        }
    }

    private InstanceConfig initGlobalConfig(Properties properties) {
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        InstanceConfig globalConfig = new InstanceConfig();
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            globalConfig.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(lazyStr)) {
            globalConfig.setLazy(Boolean.valueOf(lazyStr));
        }

        String managerAddress = getProperty(properties,
                CanalConstants.getInstanceManagerAddressKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(managerAddress)) {
            if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                managerAddress = adminManagerAddress;
            }

            globalConfig.setManagerAddress(managerAddress);
        }

        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(springXml)) {
            globalConfig.setSpringXml(springXml);
        }

        instanceGenerator = destination -> {
            InstanceConfig config = instanceConfigs.get(destination);
            if (config == null) {
                throw new CanalServerException("can't find destination:" + destination);
            }

            if (config.getMode().isManager()) {
                PlainCanalInstanceGenerator instanceGenerator = new PlainCanalInstanceGenerator(properties);
                instanceGenerator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                instanceGenerator.setSpringXml(config.getSpringXml());
                return instanceGenerator.generate(destination);
            } else if (config.getMode().isSpring()) {
                SpringCanalInstanceGenerator instanceGenerator = new SpringCanalInstanceGenerator();
                instanceGenerator.setSpringXml(config.getSpringXml());
                return instanceGenerator.generate(destination);
            } else {
                throw new UnsupportedOperationException("unknown mode :" + config.getMode());
            }

        };

        return globalConfig;
    }

    private PlainCanalConfigClient getManagerClient(String managerAddress) {
        return new PlainCanalConfigClient(managerAddress, this.adminUser, this.adminPasswd, this.registerIp, adminPort);
    }

    private void initInstanceConfig(Properties properties) {
        String destinationStr = getDestinations(properties);
        String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);

        for (String destination : destinations) {
            InstanceConfig config = parseInstanceConfig(properties, destination);
            InstanceConfig oldConfig = instanceConfigs.put(destination, config);

            if (oldConfig != null) {
                logger.warn("destination:{} old config:{} has replace by new config:{}", destination, oldConfig, config);
            }
        }
    }

    private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        InstanceConfig config = new InstanceConfig(globalInstanceConfig);
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(destination));
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            config.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            config.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(destination));
        if (!StringUtils.isEmpty(lazyStr)) {
            config.setLazy(Boolean.valueOf(lazyStr));
        }

        if (config.getMode().isManager()) {
            String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(destination));
            if (StringUtils.isNotEmpty(managerAddress)) {
                if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                    managerAddress = adminManagerAddress;
                }
                config.setManagerAddress(managerAddress);
            }
        }

        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(destination));
        if (StringUtils.isNotEmpty(springXml)) {
            config.setSpringXml(springXml);
        }

        return config;
    }

    public static String getProperty(Properties properties, String key, String defaultValue) {
        String value = getProperty(properties, key);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }


    // 配置优先级：JVM参数 > 环境变量 > 配置文件
    public static String getProperty(Properties properties, String key) {
        key = StringUtils.trim(key);
        // 优先级1: System.getProperty (JVM参数 -D)
        String value = System.getProperty(key);

        // 优先级2: System.getenv (环境变量)
        if (value == null) {
            value = System.getenv(key);
        }

        // 优先级3: Properties (配置文件)
        if (value == null) {
            value = properties.getProperty(key);
        }

        return StringUtils.trim(value);
    }

    public static String getDestinations(Properties properties) {
        String expr = getProperty(properties, CANAL_DESTINATIONS_EXPR);
        if (StringUtils.isNotBlank(expr)) {
            return parseExpr(expr);
        } else {
            return getProperty(properties, CANAL_DESTINATIONS);
        }
    }

    // ==destinations表达式解析==
    // 解析格式: prefix{start-end}
    // 例如: order{1-3} → order1,order2,order3
    //
    // 表达式解析示例：
    //   # canal.properties
    //   canal.destinations = order{1-3},payment{1-2}
    //
    //   # 等价于
    //   canal.destinations = order1,order2,order3,payment1,payment2
    private static String parseExpr(String expr) {
        String prefix = StringUtils.substringBefore(expr, "{");
        String range = StringUtils.substringAfter(expr, "{");
        range = StringUtils.substringBefore(range, "}");

        String regex = "(\\d+)-(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(range);
        if (matcher.find()) {
            String head = matcher.group(1); // 起始数字
            String tail = matcher.group(2); // 结束数字
            int start = Integer.parseInt(head);
            int end = Integer.parseInt(tail);

            List<String> list = new ArrayList<>();
            for (int i = start; i <= end; i++) {
                String d = prefix + i;
                list.add(d);
            }
            return list.stream().map(Object::toString).collect(Collectors.joining(","));
        } else {
            throw new CanalServerException("invalid destinations expr " + expr);
        }
    }

    public void start() throws Throwable {
        logger.info("## start the canal server[{}({}):{}]", ip, registerIp, port);

        // ========1. 创建Canal Server在ZooKeeper中的节点========
        final String path = ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port);
        initCid(path);

        // ========2. 订阅ZooKeeper状态变化========
        if (zkclientx != null) {
            this.zkclientx.subscribeStateChanges(new IZkStateListener() {
                @Override
                public void handleStateChanged(KeeperState state) throws Exception {

                }
                @Override
                public void handleNewSession() throws Exception {
                    // 重连后重新创建节点
                    initCid(path);
                }
                @Override
                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                    logger.error("failed to connect to zookeeper", error);
                }
            });
        }

        // ========3. 启动CanalServerWithEmbedded========
        embeddedCanalServer.start();

        // ========4. 启动所有非lazy的Instance========
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
            final String destination = entry.getKey();
            InstanceConfig config = entry.getValue();

            // 创建destination的工作节点
            if (!embeddedCanalServer.isStart(destination)) {
                // 通过HA机制启动（需要抢占ZooKeeper锁）
                ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                if (!config.getLazy() && !runningMonitor.isStart()) {
                    runningMonitor.start(); // 触发HA抢占
                }
            }

            // ========5. 注册配置监控========
            if (autoScan) {
                instanceConfigMonitors.get(config.getMode()).register(destination, defaultAction);
            }
        }

        // ========6. 启动配置监控器========
        if (autoScan) {
            instanceConfigMonitors.get(globalInstanceConfig.getMode()).start();
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (!monitor.isStart()) {
                    monitor.start();
                }
            }
        }

        // ========7. 启动Netty服务========
        if (canalServer != null) {
            canalServer.start(); // 监听11111端口
        }
    }

    public void stop() throws Throwable {

        if (canalServer != null) {
            canalServer.stop();
        }

        if (autoScan) {
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (monitor.isStart()) {
                    monitor.stop();
                }
            }
        }

        for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        }

        // 释放canal的工作节点
        releaseCid(ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port));
        logger.info("## stop the canal server[{}({}):{}]", ip, registerIp, port);

        if (zkclientx != null) {
            zkclientx.close();
        }

        // 关闭时清理缓存
        if (instanceConfigs != null) {
            instanceConfigs.clear();
        }
        if (managerClients != null) {
            managerClients.clear();
        }
        if (instanceConfigMonitors != null) {
            instanceConfigMonitors.clear();
        }

        ZkClientx.clearClients();

        // 需要释放 CanalServerWithEmbedded 否则主线程退出后，进程无法自动完整退出...
        if (embeddedCanalServer != null && embeddedCanalServer.isStart()) {
            embeddedCanalServer.stop();
        }
    }

    private void initCid(String path) {
        // logger.info("## init the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            try {
                zkclientx.createEphemeral(path);
            } catch (ZkNoNodeException e) {
                // 如果父目录不存在，则创建
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                zkclientx.createPersistent(parentDir, true);
                zkclientx.createEphemeral(path);
            } catch (ZkNodeExistsException e) {
                // ignore
                // 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
            }

        }
    }

    private void releaseCid(String path) {
        // logger.info("## release the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            zkclientx.delete(path);
        }
    }

    public CanalMQStarter getCanalMQStarter() {
        return canalMQStarter;
    }

    public void setCanalMQStarter(CanalMQStarter canalMQStarter) {
        this.canalMQStarter = canalMQStarter;
    }

    public Map<InstanceMode, InstanceConfigMonitor> getInstanceConfigMonitors() {
        return instanceConfigMonitors;
    }

    public Map<String, InstanceConfig> getInstanceConfigs() {
        return instanceConfigs;
    }

}
