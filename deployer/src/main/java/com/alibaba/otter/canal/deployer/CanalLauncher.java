package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * CanalLauncher是整个服务的入口类
 */
public class CanalLauncher {
    private static final Logger logger = LoggerFactory.getLogger(CanalLauncher.class);

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    public static final CountDownLatch runningLatch = new CountDownLatch(1);
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            1, new NamedThreadFactory("canal-server-scan"));

    public static void main(String[] args) {
        try {
            // ========第1步：设置全局异常处理器========
            logger.info("## set default uncaught exception handler");
            setGlobalUncaughtExceptionHandler();

            // 支持rocketmq客户端配置日志路径
            System.setProperty("rocketmq.client.logUseSlf4j", "true");

            // ========第2步：读取配置文件========
            logger.info("## load canal configurations");
            /*
            配置文件路径优先级：
              1. JVM参数: -Dcanal.conf=/path/to/canal.properties
              2. 默认值:   classpath:canal.properties
             */
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                // 从classpath加载（打包后）
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                // 从文件系统加载（开发调试时）
                properties.load(new FileInputStream(conf));
            }

            // ========第3步：判断是否使用Admin管理模式========
            /*
            读取 canal.admin.manager 配置
              - 有值：Admin 远程管理模式（从 Admin 拉取配置）
              - 无值：本地配置模式（使用本地 canal.properties）
             */
            final CanalStarter canalStater = new CanalStarter(properties);
            /*
            根据canal.admin.manager是否为空判断是否是admin控制，如果不是admin控制，就直接根据canal.properties的配置
            来，如果是admin控制，使用PlainCanalConfigClient获取远程配置，新开一个线程池每隔5秒用http请求去admin上拉
            配置进行merge（这里依赖了instance模块的相关配置拉取的工具方法）用md5进行校验，如果canal-server配置有更新，
            那么就重启canal-server
             */
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                // ===Admin模式：从远程Admin配置中心拉取配置===
                String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
                String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
                if (StringUtils.isEmpty(passwd)) {
                    throw new IllegalArgumentException("canal.admin.passwd is empty , pls check https://github.com/alibaba/canal/issues/4941");
                }
                String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
                boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
                String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
                String name = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_REGISTER_NAME);
                if (StringUtils.isEmpty(name)) {
                    name = AddressUtils.getHostName();
                }

                // 获取本机IP和主机名
                String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
                if (StringUtils.isEmpty(registerIp)) {
                    registerIp = AddressUtils.getHostIp();
                }

                // 创建配置客户端，从Admin拉取配置
                final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress, user,
                        passwd, registerIp, Integer.parseInt(adminPort), autoRegister, autoCluster, name);
                // 获取初始配置
                PlainCanal canalConfig = configClient.findServer(null);
                if (canalConfig == null) {
                    throw new IllegalArgumentException("managerAddress:" + managerAddress + " can't not found config for [" + registerIp + ":" + adminPort + "]");
                }
                Properties managerProperties = canalConfig.getProperties();
                // 合并本地配置（本地配置优先级更高）
                managerProperties.putAll(properties);

                // 启动定时扫描线程，每隔5秒从Admin拉取最新配置
                int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(
                        managerProperties, CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
                executor.scheduleWithFixedDelay(new Runnable() {
                    private PlainCanal lastCanalConfig;
                    @Override
                    public void run() {
                        try {
                            if (lastCanalConfig == null) {
                                lastCanalConfig = configClient.findServer(null);
                            } else {
                                // 使用MD5判断配置是否有变更
                                PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                                if (newCanalConfig != null) {
                                    // 远程配置变更，重启整个Server
                                    canalStater.stop();
                                    Properties managerProperties = newCanalConfig.getProperties();
                                    managerProperties.putAll(properties);
                                    canalStater.setProperties(managerProperties);
                                    canalStater.start();
                                    lastCanalConfig = newCanalConfig;
                                }
                            }
                        } catch (Throwable e) {
                            logger.error("scan failed", e);
                        }
                    }
                }, 0, scanIntervalInSecond, TimeUnit.SECONDS);

                canalStater.setProperties(managerProperties);
            } else {
                // ===本地模式：使用本地canal.properties配置（单机版走这里）===
                canalStater.setProperties(properties);
            }

            // ========第4步：启动Canal Server========
            canalStater.start();

            // ========第5步：保持主线程存活========
            /*
            使用CountDownLatch阻塞主线程，让服务持续运行
            等价于while(true)循环，但更优雅
             */
            runningLatch.await();
            executor.shutdownNow();
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
        }
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));
    }

}
