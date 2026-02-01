package com.alibaba.otter.canal.server.embedded;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.SecurityUtil;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.CanalService;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.spi.CanalMetricsProvider;
import com.alibaba.otter.canal.spi.CanalMetricsService;
import com.alibaba.otter.canal.spi.NopCanalMetricsService;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MigrateMap;

/**
 * CanalServerWithEmbedded会根据请求携带的destination参数，选择对应的CanalInstance来真正的处理请求。
 * CanalServer中最重要的是CanalServerWithEmbedded，而CanalServerWithEmbedded中最重要的是CanalInstance。
 */
public class CanalServerWithEmbedded extends AbstractCanalLifeCycle implements CanalServer, CanalService {
    private static final Logger logger = LoggerFactory.getLogger(CanalServerWithEmbedded.class);

    /*
    CanalServer包含多个Instance，canalInstances记录了instance名称与实例的映射关系。
    因为是一个Map，所以同一个Server不允许出现相同instance名称，比如不能同时有两个example
    在一个server上。但是允许一个Server上有example1和example2。
    根据客户端请求携带的destination参数将其转发到对应的CanalInstance上去处理。
     */
    private Map<String/*destination*/, CanalInstance> canalInstances;
    private CanalInstanceGenerator canalInstanceGenerator;

    private int metricsPort;
    private CanalMetricsService metrics = NopCanalMetricsService.NOP;

    private String user;
    private String passwd;

    public CanalServerWithEmbedded() {
        // 希望也保留用户new单独实例的需求,兼容历史
    }

    private static class SingletonHolder {
        private static final CanalServerWithEmbedded CANAL_SERVER_WITH_EMBEDDED = new CanalServerWithEmbedded();
    }

    public static CanalServerWithEmbedded instance() {
        return SingletonHolder.CANAL_SERVER_WITH_EMBEDDED;
    }

    @Override
    public void start() {
        if (!isStart()) {
            super.start();

            // Canal使用SPI机制加载MetricsProvider，支持第三方扩展
            // 启动Metrics Service
            if (metricsPort > 0) {
                loadCanalMetrics(); // ServiceLoader加载Provider
                metrics.setServerPort(metricsPort);
                metrics.initialize();
            }

            canalInstances = MigrateMap.makeComputingMap(destination -> canalInstanceGenerator.generate(destination));
        }
    }

    @Override
    public void stop() {
        super.stop();
        for (Map.Entry<String, CanalInstance> entry : canalInstances.entrySet()) {
            try {
                CanalInstance instance = entry.getValue();
                if (instance.isStart()) {
                    try {
                        String destination = entry.getKey();
                        MDC.put("destination", destination);
                        entry.getValue().stop();
                        logger.info("stop CanalInstances[{}] successfully", destination);
                    } finally {
                        MDC.remove("destination");
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("stop CanalInstance[%s] has an error", entry.getKey()), e);
            }
        }
        metrics.terminate();
    }

    public boolean auth(String user, String passwd, byte[] seed) {
        // 如果user/passwd密码为空,则任何用户账户都能登录
        if ((StringUtils.isEmpty(this.user) || StringUtils.equals(this.user, user))) {
            if (StringUtils.isEmpty(this.passwd)) {
                return true;
            } else if (StringUtils.isEmpty(passwd)) {
                // 如果server密码有配置,客户端密码为空,则拒绝
                return false;
            }

            try {
                byte[] passForClient = SecurityUtil.hexStr2Bytes(passwd);
                return SecurityUtil.scrambleServerAuth(passForClient, SecurityUtil.hexStr2Bytes(this.passwd), seed);
            } catch (NoSuchAlgorithmException e) {
                return false;
            }
        }

        return false;
    }

    public void start(final String destination) {
        final CanalInstance canalInstance = canalInstances.get(destination);
        if (!canalInstance.isStart()) {
            try {
                MDC.put("destination", destination);
                if (metrics.isRunning()) {
                    metrics.register(canalInstance);
                }
                canalInstance.start();
                logger.info("start CanalInstances[{}] successfully", destination);
            } finally {
                MDC.remove("destination");
            }
        }
    }

    public void stop(String destination) {
        CanalInstance canalInstance = canalInstances.remove(destination);
        if (canalInstance != null) {
            if (canalInstance.isStart()) {
                try {
                    MDC.put("destination", destination);
                    canalInstance.stop();
                    if (metrics.isRunning()) {
                        metrics.unregister(canalInstance);
                    }
                    logger.info("stop CanalInstances[{}] successfully", destination);
                } finally {
                    MDC.remove("destination");
                }
            }
        }
    }

    public boolean isStart(String destination) {
        return canalInstances.containsKey(destination) && canalInstances.get(destination).isStart();
    }

    /**
     * ==提供订阅方法的作用是：MySQL新增了一张表，客户端原先没有同步这张表，现在需要同步，所以需要重新订阅==
     * subscribe主要用于处理客户端的订阅请求，目前情况下，一个CanalInstance只能由一个客户端订阅，不过可以
     * 重复订阅，重复订阅时会更新对应的filter信息。
     *
     * @param clientIdentity 表示Canal Client客户端，从中可以获取出客户端指定连接的Destination
     *                       CanalServerWithEmbedded记录了每个Destination对应的Instance，可以获取客户端对应的Instance
     */
    @Override
    public void subscribe(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());

        // STEP 1：【启动元数据管理器】根据客户端要订阅的destination，找到对应的CanalInstance，启动元数据管理器
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        if (!canalInstance.getMetaManager().isStart()) {
            canalInstance.getMetaManager().start();
        }

        // STEP 2：【记录订阅关系】通过CanalInstance的CanalMetaManager组件进行元数据管理，记录一下当前这个CanalInstance有客户端在订阅
        canalInstance.getMetaManager().subscribe(clientIdentity);

        /*
        STEP 3：【获取消费起始位置】
        获取客户端当前订阅的binlog位置(Position)，首先尝试从CanalMetaManager中获取
        CanalMetaManager中记录了某个client当前订阅binlog的位置信息。如果是第一次订阅，肯定无
        法获取到这个位置，则尝试从CanalEventStore中获取第一个binlog的位置。从CanalEventStore
        中获取binlog位置信息的逻辑是：CanalInstance一旦启动，就会立刻去拉取binlog，存储到CanalEventStore
        中，在第一次订阅的情况下，CanalEventStore中的第一条binlog的位置，就是当前客户端当前消费的开始位置。
         */
        Position position = canalInstance.getMetaManager().getCursor(clientIdentity);
        if (position == null) {
            // 如果是第一次订阅，尝试从CanalEventStore中获取第一个binlog的位置，作为客户端订阅开始的位置
            position = canalInstance.getEventStore().getFirstPosition();
            if (position != null) {
                canalInstance.getMetaManager().updateCursor(clientIdentity, position);
            }
            logger.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
        } else {
            logger.info("subscribe successfully, {} use last cursor position:{} ", clientIdentity, position);
        }

        // STEP 4：【通知Instance订阅变更（更新filter）】
        canalInstance.subscribeChange(clientIdentity);
    }

    /**
     * unsubscribe方法主要用于取消订阅关系，其实就是找到CanalInstance对应的CanalMetaManager，
     * 调用其unsubscribe取消这个订阅记录。需要注意的是，取消订阅并不意味着停止CanalInstance。
     * 当某个客户端取消了订阅，还会有新的client来订阅这个CanalInstance，所以不能停。
     */
    @Override
    public void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        canalInstance.getMetaManager().unsubscribe(clientIdentity);
        logger.info("unsubscribe successfully, {}", clientIdentity);
    }

    /**
     * 这一个管理方法，其作用是列出订阅某个destination的所有client。这里返回的是一个List<ClientIdentity>，
     * 不过我们已经多次提到，目前一个destination只能由一个client订阅。这里之所以返回一个list，是canal原先
     * 计划要支持多个client订阅同一个destination。不过，这个功能一直没有实现。所以List中，实际上只会包含一
     * 个ClientIdentity。
     */
    public List<ClientIdentity> listAllSubscribe(String destination) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(destination);
        return canalInstance.getMetaManager().listAllSubscribeInfo(destination);
    }

    /**
     * 与getWithoutAck主要流程完全相同，唯一不同的是，在返回数据给用户前，直接进行了ack，而不管客户端消费是否成功。
     *
     * <pre>
     * 注意：meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步。
     *      (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return get(clientIdentity, batchSize, null, null);
    }

    /**
     * 获取数据，可以指定超时时间.
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意：meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步.
     *      (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        synchronized (canalInstance) {
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);
            if (positionRanges != null) {
                throw new CanalServerException(String.format("clientId:%s has last batch:[%s] isn't ack , maybe loss data",
                        clientIdentity.getClientId(), positionRanges));
            }

            Events<Event> events = null;
            Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
            events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);

            if (CollectionUtils.isEmpty(events.getEvents())) {
                logger.debug("get successfully, clientId:{} batchSize:{} but result is null",
                             clientIdentity.getClientId(), batchSize);
                return new Message(-1, true, new ArrayList()); // 返回空包，避免生成batchId，浪费性能
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                boolean raw = isRaw(canalInstance.getEventStore());
                List entrys = null;
                if (raw) {
                    // new list
                    entrys = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());
                } else {
                    entrys = events.getEvents().stream().map(Event::getEntry).collect(Collectors.toList());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("get successfully, clientId:{} batchSize:{} real size is {} and result is [batchId:{} , position:{}]",
                            clientIdentity.getClientId(), batchSize, entrys.size(), batchId, events.getPositionRange());
                }

                // 直接提交ack
                ack(clientIdentity, batchId);
                return new Message(batchId, raw, entrys);
            }
        }
    }

    /**
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return getWithoutAck(clientIdentity, batchSize, null, null);
    }

    /**
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        synchronized (canalInstance) {
            // 检查是否有未ack的批次
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);
            Events<Event> events = null;
            if (positionRanges != null) {
                // 从上次未ack的位置继续获取
                events = getEvents(canalInstance.getEventStore(), positionRanges.getStart(), batchSize, timeout, unit);
            } else {
                // ack后的第一次获取
                Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
                if (start == null) { // 第一次，还没有过ack记录，则获取当前store中的第一条
                    start = canalInstance.getEventStore().getFirstPosition();
                }
                events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
            }

            if (CollectionUtils.isEmpty(events.getEvents())) { // 空消息
                // 返回空包，避免生成batchId，浪费性能
                return new Message(-1, true, new ArrayList());
            } else {
                // 记录批次信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                // 是否开启raw模式，用于控制entry的存储方式，默认是开启的
                boolean raw = isRaw(canalInstance.getEventStore());
                List entrys = null;
                if (raw) {
                    entrys = events.getEvents().stream().map(Event::getRawEntry).collect(Collectors.toList());
                } else {
                    entrys = events.getEvents().stream().map(Event::getEntry).collect(Collectors.toList());
                }

                if (logger.isInfoEnabled()) {
                    logger.info("getWithoutAck successfully, clientId:{} batchSize:{}  real size is {} and result is [batchId:{} , position:{}]",
                            clientIdentity.getClientId(),
                            batchSize,
                            entrys.size(),
                            batchId,
                            events.getPositionRange());
                }

                // 返回消息
                return new Message(batchId, raw, entrys);
            }

        }
    }

    /**
     * 查询当前未被ack的batch列表，batchId会按照从小到大进行返回
     */
    public List<Long> listBatchIds(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        Map<Long, PositionRange> batchs = canalInstance.getMetaManager().listAllBatchs(clientIdentity);
        List<Long> result = new ArrayList<>(batchs.keySet());
        Collections.sort(result);
        return result;
    }

    /**
     * 进行batch id的确认。确认之后，小于等于此batchId的Message都会被确认。
     *
     * <pre>
     * 注意：进行反馈时必须按照batchId的顺序进行ack(需有客户端保证)
     * </pre>
     */
    @Override
    public void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

        // 移除批次记录
        PositionRange<LogPosition> positionRanges = null;
        positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity, batchId); // 更新位置
        if (positionRanges == null) { // 说明是重复的ack/rollback
            throw new CanalServerException(String.format("ack error , clientId:%s batchId:%d is not exist , " +
                            "please check", clientIdentity.getClientId(), batchId));
        }

        // 更新消费cursor
        if (positionRanges.getAck() != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck());
            if (logger.isInfoEnabled()) {
                logger.info("ack successfully, clientId:{} batchId:{} position:{}",
                        clientIdentity.getClientId(), batchId, positionRanges);
            }
        }

        // 清理EventStore中已确认的数据
        canalInstance.getEventStore().ack(positionRanges.getEnd(), positionRanges.getEndSeq());
    }

    /**
     * ==回滚到未ack的位置==
     * 客户端消费失败时调用，下次get从上次ack位置重新开始。
     * <p>
     * 回滚到未进行{@link #ack}的地方，下次fetch的时候，可以从最后一个没有{@link #ack}的地方开始拿
     */
    @Override
    public void rollback(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return; // 未订阅，忽略
        }

        synchronized (canalInstance) {
            // 清除batch信息
            canalInstance.getMetaManager().clearAllBatchs(clientIdentity);
            // 回滚eventStore中的状态信息
            canalInstance.getEventStore().rollback();
            logger.info("rollback successfully, clientId:{}", new Object[]{clientIdentity.getClientId()});
        }
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    @Override
    public void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }
        synchronized (canalInstance) {
            // 清除batch信息
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity,
                    batchId);
            if (positionRanges == null) { // 说明是重复的ack/rollback
                throw new CanalServerException(String.format("rollback error, clientId:%s batchId:%d is not exist , please check",
                        clientIdentity.getClientId(),
                        batchId));
            }

            // lastRollbackPostions.put(clientIdentity,
            // positionRanges.getEnd());// 记录一下最后rollback的位置
            // TODO 后续rollback到指定的batchId位置
            canalInstance.getEventStore().rollback();// rollback
            // eventStore中的状态信息
            logger.info("rollback successfully, clientId:{} batchId:{} position:{}",
                    clientIdentity.getClientId(),
                    batchId,
                    positionRanges);
        }
    }

    public Map<String, CanalInstance> getCanalInstances() {
        return Maps.newHashMap(canalInstances);
    }

    // ======================== helper method =======================

    /**
     * 根据不同的参数，选择不同的方式获取数据
     */
    private Events<Event> getEvents(CanalEventStore eventStore, Position start, int batchSize, Long timeout,
                                    TimeUnit unit) {
        if (timeout == null) {
            return eventStore.tryGet(start, batchSize);
        } else {
            try {
                if (timeout <= 0) {
                    return eventStore.get(start, batchSize);
                } else {
                    return eventStore.get(start, batchSize, timeout, unit);
                }
            } catch (Exception e) {
                throw new CanalServerException(e);
            }
        }
    }

    private void checkSubscribe(ClientIdentity clientIdentity) {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            throw new CanalServerException(String.format("ClientIdentity:%s should subscribe first",
                    clientIdentity.toString()));
        }
    }

    private void checkStart(String destination) {
        if (!isStart(destination)) {
            throw new CanalServerException(String.format("destination:%s should start first", destination));
        }
    }

    // ==SPI加载MetricsProvider==
    private void loadCanalMetrics() {
        ServiceLoader<CanalMetricsProvider> providers = ServiceLoader.load(CanalMetricsProvider.class);
        List<CanalMetricsProvider> list = new ArrayList<>();
        for (CanalMetricsProvider provider : providers) {
            list.add(provider);
        }

        if (list.isEmpty()) {
            return;
        }

        // // 只允许一个Provider
        if (list.size() > 1) {
            logger.warn("Found more than one CanalMetricsProvider, use the first one.");
            // 报告冲突
            for (CanalMetricsProvider p : list) {
                logger.warn("Found CanalMetricsProvider: {}.", p.getClass().getName());
            }
        }

        CanalMetricsProvider provider = list.get(0);
        this.metrics = provider.getService();
    }

    private boolean isRaw(CanalEventStore eventStore) {
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            return ((MemoryEventStoreWithBuffer) eventStore).isRaw();
        }

        return true;
    }

    // ========= setter ==========

    public void setCanalInstanceGenerator(CanalInstanceGenerator canalInstanceGenerator) {
        this.canalInstanceGenerator = canalInstanceGenerator;
    }

    public void setMetricsPort(int metricsPort) {
        this.metricsPort = metricsPort;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

}
