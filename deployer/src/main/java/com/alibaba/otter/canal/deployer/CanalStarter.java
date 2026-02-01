package com.alibaba.otter.canal.deployer;

import com.alibaba.otter.canal.connector.core.spi.ProxyCanalMQProducer;
import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.server.CanalMQStarter;

/**
 * CanalServer启动器
 */
public class CanalStarter {
    private static final Logger logger = LoggerFactory.getLogger(CanalStarter.class);

    private static final String CONNECTOR_SPI_DIR = "/plugin";
    private static final String CONNECTOR_STANDBY_SPI_DIR = "/canal/plugin";

    private CanalController controller = null;
    private CanalMQProducer canalMQProducer = null;
    private CanalMQStarter canalMQStarter = null;

    private Thread shutdownThread = null;
    private volatile boolean running = false;
    private volatile Properties properties;

    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties) {
        this.properties = properties;
    }

    public synchronized void start() throws Throwable {
        /*
        ========第1步：判断Server模式========
        canal.serverMode 可选值：
          - tcp: 默认模式，使用Netty提供TCP服务
          - kafka: 将binlog发送到Kafka
          - rocketMQ: 将binlog发送到RocketMQ
          - rabbitMQ: 将binlog发送到RabbitMQ
          - pulsarMQ: 将binlog发送到Pulsar
         */
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);

        // ========第2步：如果不是TCP模式，加载MQ Producer========
        if (!"tcp".equalsIgnoreCase(serverMode)) {
            // 使用SPI机制加载对应MQ的Producer
            ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
            canalMQProducer = loader.getExtension(
                    serverMode.toLowerCase(),
                    CONNECTOR_SPI_DIR,        // /plugin
                    CONNECTOR_STANDBY_SPI_DIR // /canal/plugin
            );
            if (canalMQProducer != null) {
                canalMQProducer =  new ProxyCanalMQProducer(canalMQProducer);
                canalMQProducer.init(properties); // 初始化MQ Producer
            }
        }

        // ========第3步：如果配置了MQ Producer，禁用Netty========
        if (canalMQProducer != null) {
            MQProperties mqProperties = canalMQProducer.getMqProperties();
            // MQ模式下不需要Netty服务
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            if (mqProperties.isFlatMessage()) {
                // 扁平化消息模式，设置raw=false，避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
            }
        }

        // ========第4步：创建并启动CanalController（init-start模型）========
        logger.info("## start the canal server.");
        controller = new CanalController(properties); // init： 构造函数会完成大量的初始化工作
        controller.start();                           // start: 启动CanalController
        logger.info("## the canal server is running now ......");

        /*
        ========第5步：注册ShutdownHook========
        在JVM退出时自动执行清理工作，保证服务能够优雅关闭
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal server");
                controller.stop();
                CanalLauncher.runningLatch.countDown(); // 释放主线程
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal Server:", e);
            } finally {
                logger.info("## canal server is down.");
            }
        }));

        // ========第6步：启动MQ Starter（如果配置了MQ）========
        if (canalMQProducer != null) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            String destinations = CanalController.getDestinations(properties);
            canalMQStarter.start(destinations);
            controller.setCanalMQStarter(canalMQStarter);
        }

        // ========第7步：启动Admin服务（如果配置了）========
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);
            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);

            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}", port, user, passwd, ip);

            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.parseInt(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }

        running = true;
    }

    public boolean isRunning() {
        return running;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public CanalController getController() {
        return controller;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }
}
