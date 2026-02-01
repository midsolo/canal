package com.alibaba.otter.canal.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * MemoryEventStoreWithBuffer是目前开源版本中的CanalEventStore接口的唯一实现，基于内存模式。当然你也可以进行扩展，
 * 提供一个基于本地文件存储方式的CanalEventStore实现。这样就可以一份数据让多个业务消费者进行订阅，只要独立维护消费
 * 位置元数据即可。然而，我不得不提醒你的是，基于本地文件的存储方式，一定要考虑好数据清理工作，否则会有大坑。如果一个
 * 库只有一个业务方订阅，其实根本也不用实现本地存储，使用基于内存模式的队列进行缓存即可。如果client消费的快，那么队列
 * 中的数据放入后就被取走，队列基本上一直是空的，实现本地存储也没意义；如果client消费的慢，队列基本上一直是满的，只要
 * client来获取，总是能拿到数据，因此也没有必要实现本地存储。
 * =====================================================================================================================
 * 针对这个环形队列，Canal定义了3类操作：Put、Get、Ack，其中：
 * Put操作：添加数据。event parser模块拉取到binlog后，并经过event sink模块过滤，最终就通过Put操作存储到了队列中。
 * Get操作：获取数据。canal client连接到canal server后，最终获取到的binlog都是从这个队列中取得。
 * Ack操作：确认消费成功。canal client获取到binlog事件消费后，需要进行Ack。你可以认为Ack操作实际上就是将消费成功
 * 的事件从队列中删除，如果一直不Ack的话，队列满了之后，Put操作就无法添加新的数据了。
 *
 * 对应的，我们需要使用3个变量来记录Put、Get、Ack这三个操作的位置，其中：
 * putSequence: 每放入一个数据putSequence +1，可表示存储数据存储的总数量
 * getSequence: 每获取一个数据getSequence +1，可表示数据订阅获取的最后一次提取位置
 * ackSequence: 每确认一个数据ackSequence + 1，可表示数据最后一次消费成功位置
 *
 * 另外，putSequence、getSequence、ackSequence这3个变量初始值都是-1，且都是递增的，均用long型表示。由于数据只有被
 * Put进来后，才能进行Get；Get之后才能进行Ack。所以，这三个变量满足以下关系：
 * ackSequence <= getSequence <= putSequence
 * =====================================================================================================================
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 * 2. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，
 *    如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge
        implements CanalEventStore<Event>, CanalStoreScavenge {

    private static final long INIT_SEQUENCE = -1;

    /*
      环形队列示意图：

           索引:    0     1     2    ...  16383
                 ┌─────┬─────┬─────┬─────┬─────┐
       entries:  │     │     │     │     │     │
                 └─────┴─────┴─────┴─────┴─────┘
                          ▲
                          │
              这是一个循环使用的数组

      putSequence:  10000  （放入了10001个Event）
      getSequence:  9995   （取到了第9996个Event）
      ackSequence:  9990   （确认了9991个Event）

      队列中:
      - [9991, 9990]: 已确认，可被覆盖
      - [9996, 10000]: 已取未确认
      - [10001, ]: 可供获取
     */

    // 表示RingBuffer使用的内存单元, 默认是1kb，和canal.instance.memory.buffer.size组合决定最终的内存使用大小
    private int bufferMemUnit = 1024;
    // 表示RingBuffer队列的最大容量，也就是可缓存的binlog事件的最大记录数，默认值为16384
    private int bufferSize = 16 * 1024; // 环形队列大小（默认16384）
    // ==环形队列底层基于的Event[]数组，队列的大小就是bufferSize==
    private Event[] entries;            // 环形队列底层数组
    // 用于对putSequence、getSequence、ackSequence进行取余操作
    private int indexMask;

    // 三个关键序号（初始值为-1），记录put/get/ack操作的三个下标
    private AtomicLong putSequence = new AtomicLong(INIT_SEQUENCE); // 代表当前put操作最后一次写操作发生的位置
    private AtomicLong getSequence = new AtomicLong(INIT_SEQUENCE); // 代表当前get操作读取的最后一条的位置
    private AtomicLong ackSequence = new AtomicLong(INIT_SEQUENCE); // 代表当前ack操作的最后一条的位置
    // 满足关系: ackSequence <= getSequence <= putSequence

    // 记录put/get/ack操作的event占用内存的累加值，都是从0开始计算，batchMode=MEMSIZE才生效
    private AtomicLong putMemSize = new AtomicLong(0);
    private AtomicLong getMemSize = new AtomicLong(0);
    private AtomicLong ackMemSize = new AtomicLong(0);

    // 记录下put/get/ack操作的三个execTime
    private AtomicLong putExecTime = new AtomicLong(System.currentTimeMillis());
    private AtomicLong getExecTime = new AtomicLong(System.currentTimeMillis());
    private AtomicLong ackExecTime = new AtomicLong(System.currentTimeMillis());

    // 记录下put/get/ack操作的三个table rows
    private AtomicLong putTableRows = new AtomicLong(0);
    private AtomicLong getTableRows = new AtomicLong(0);
    private AtomicLong ackTableRows = new AtomicLong(0);

    // 并发控制：阻塞put/get操作控制信号
    private ReentrantLock lock     = new ReentrantLock(); // put操作和get操作共用一把锁(lock)
    private Condition     notFull  = lock.newCondition(); // 队列满时，put操作等待 | 用于控制put操作，只有队列没满的情况下才能put
    private Condition     notEmpty = lock.newCondition(); // 队列空时，get操作等待 | 控制get操作，只有队列不为空的情况下，才能get

    /*
    表示Canal内存store中数据缓存模式，支持两种方式：
    ITEMSIZE : 根据buffer.size进行限制，只限制记录的数量。这种方式有一些潜在的问题，举个极端例子，假设每个event有1M，
    那么16384个这种event占用内存要达到16G左右，基本上肯定会造成内存溢出(超大内存的物理机除外)。
    MEMSIZE : 根据buffer.size * buffer.memunit的大小，限制缓存记录占用的总内存大小。指定为这种模式时，意味着默认缓存
    的event占用的总内存不能超过16384*1024=16M。这个值偏小，但笔者认为也足够了。因为通常我们在一个服务器上会部署多个
    instance，每个instance的store模块都会占用16M，因此只要instance的数量合适，也就不会浪费内存了。部分读者可能会担心，
    这是否限制了一个event的最大大小为16M，实际上是没有这个限制的。因为canal在Put一个新的event时，只会判断队列中已有的
    event占用的内存是否超过16M，如果没有，新的event不论大小是多少，总是可以放入的(canal的内存计算实际上是不精确的)，之
    后的event再要放入时，如果这个超过16M的event没有被消费，则需要进行等待。
     */
    private BatchMode batchMode = BatchMode.ITEMSIZE; // 表示Canal内存store中数据缓存模式，默认为内存大小模式
    private boolean ddlIsolation = false;             // 对于Get操作生效，用于设置ddl语句是否单独一个batch返回
    private boolean raw = true;                       // 是否开启raw模式，用于控制entry的存储方式

    public MemoryEventStoreWithBuffer() { }

    public MemoryEventStoreWithBuffer(BatchMode batchMode) {
        this.batchMode = batchMode;
    }


    // ==========================CanalLifeCycle==============================
    @Override // CanalLifeCycle#start
    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        indexMask = bufferSize - 1;
        // 初始化MemoryEventStoreWithBuffer内部的环形队列，其实就是初始化一下Event[]数组
        entries = new Event[bufferSize];
    }

    @Override // CanalLifeCycle#stop
    public void stop() throws CanalStoreException {
        super.stop();
        // 停止时会清空所有缓存的数据，将维护的相关状态变量设置为初始值
        cleanAll();
    }


    // ==========================CanalEventStore#put==============================
    // put操作是parser模块解析binlog事件，经过sink模块过滤后，放入到store模块中，也就是
    // 说Put操作实际上是canal内部调用
    @Override // 会一直进行阻塞，直到有足够的空间可以放入
    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }
    @Override // 超过指定时间还未put成功，会抛出InterruptedException
    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }
    @Override // 每次只是尝试放入数据，立即返回true或者false，不会阻塞
    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    @Override
    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                // 检查是否有足够的空闲位置
                while (!checkFreeSlotAt(putSequence.get() + data.size())) {
                    // 队列满则等待
                    notFull.await();
                }
            } catch (InterruptedException ie) {
                notFull.signal();
                throw ie;
            }
            // ==执行实际的put操作==
            doPut(data);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        // 如果需要插入的List为空，直接返回true
        if (data == null || data.isEmpty()) {
            return true;
        }

        // 获得超时时间，并通过加锁进行put操作
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            // 这是一个死循环，执行到下面任意一个return或者抛出异常是时才会停止
            for (; ; ) {
                // 检查是否足够的slot空位可供插入
                if (checkFreeSlotAt(putSequence.get() + data.size())) {
                    // ==调用doPut方法进行真正的数据插入==
                    doPut(data);
                    return true;
                }

                // 判断是否已经超时，如果超时，则不执行插入操作，直接返回false
                if (nanos <= 0) {
                    return false;
                }

                // 如果还没有超时，调用notFull.awaitNanos进行等待，需要其他线程调用notFull.signal方法唤醒
                try {
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notFull.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    /** ==底层都是通过调用doPut方法来完成真正的数据放入== */
    private void doPut(List<Event> data) {
        // =====1、将新插入的event数据赋值到Event[]数组的正确位置上，就算完成了插入=====
        // 获得putSequence的当前值current，和插入数据后的putSequence结束值end
        long current = putSequence.get();
        long end = current + data.size();
        // 循环写入每个Event，从current位置开始，到end位置结束
        for (long next = current + 1; next <= end; next++) {
            /*
            ==通过getIndex方法方法来进行位置转换，其内部通过位运算来快速取余数==
            通过getIndex方法对next变量转换成正确的位置，设置到Event[]数组中，需要转换的
            原因在于，这里的Event[]数组是环形队列的底层实现，其大小为bufferSize值，默认
            为16384。运行一段时间后，接收到的binlog数量肯定会超过16384，每接受到一个event，
            putSequence+1，因此最终必然超过这个值。 而next变量是比当前putSequence值要大
            的，因此必须进行转换，否则会数组越界，转换工作就是在getIndex方法中进行的。
             */
            int index = getIndex(next); // 计算环形队列中的实际位置
            entries[index] = data.get((int) (next - current - 1));
        }

        // =====2、当新插入的event记录数累加到putSequence上=====
        // 更新putSequence | 直接设置putSequence为end值，相当于完成event记录数的累加
        putSequence.set(end);

        // =====3、累加新插入的event的大小到putMemSize上=====
        if (batchMode.isMemSize()) {
            // 用于记录本次插入的event记录的大小
            long size = 0;
            // 循环每一个event
            for (Event event : data) {
                // 计算每个event的大小，并累加到size变量上
                size += calculateSize(event);
            }
            // 将size变量的值，添加到当前putMemSize
            putMemSize.getAndAdd(size);
        }

        profiling(data, OP.PUT);

        // =====4、唤醒等待的get操作 | 通知队列中有数据了，如果之前有client获取数据处于阻塞状态，将会被唤醒=====
        notEmpty.signal();
    }

    // ==========================CanalEventStore#get（客户端获取数据）==============================
    // Get操作(以及ack、rollback)，是由client发起的网络请求，server端通过对请求参数进行
    // 解析，最终调用CanalEventStore模块中定义的对应方法。类似put操作，从getSequence+1
    // 位置开始读取，读取后更新getSequence，并唤醒可能等待的put操作。
    @Override // 获取指定大小的数据，阻塞等待其操作完成
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal();
                throw ie;
            }
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    @Override // 获取指定大小的数据，阻塞等待其操作完成或者超时，如果超时了，有多少，返回多少
    public Events<Event> get(Position start, // 表示从哪个位置开始获取
                             int batchSize,  // 表示批量获取的数据大小
                             long timeout,   // 表示超时时间
                             TimeUnit unit)  // 表示超时时间单位
            throws InterruptedException, CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                // 检查是否有足够的event可供获取
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                    // 有足够的event就直接获取
                    return doGet(start, batchSize);
                }

                // 如果时间到了，有多少取多少
                if (nanos <= 0) {
                    return doGet(start, batchSize);
                }

                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal();
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override // 尝试获取，如果获取不到立即返回
    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        // =====STEP1 确定从哪个位置开始获取数据=====
        long current = getSequence.get();        // 获得当前的get位置
        long maxAbleSequence = putSequence.get();// 获得当前的put位置
        long next = current; // 要获取的第一个Event的位置，一开始等于当前get位置
        long end = current;  // 要获取的最后一个event的位置，一开始也是当前get位置，每获取一个event，end值加1，最大为current+batchSize
                             // 因为可能进行ddl隔离，因此可能没有获取到batchSize个event就返回了，此时end值就会小于current+batchSize

        // 如果startPosition为null，说明是第一次订阅，默认+1处理，因为getSequence的值是从-1开始的
        if (startPosition == null || !startPosition.getPostion().isIncluded()) {
            next = next + 1;
        }

        // 如果没有数据，直接返回一个空列表
        if (current >= maxAbleSequence) {
            return new Events<>();
        }

        // =====STEP2 如果有数据，根据batchMode是ITEMSIZE或MEMSIZE选择不同的处理方式=====
        Events<Event> result = new Events<>();
        List<Event> entrys = result.getEvents(); // 维护要返回的Event列表
        long memsize = 0;
        // 如果batchMode是ITEMSIZE
        if (batchMode.isItemSize()) {
            end = (next + batchSize - 1) < maxAbleSequence ? (next + batchSize - 1) : maxAbleSequence;
            // 循环从开始位置(next)到结束位置(end)，每次循环next+1，提取数据并返回
            for (; next <= end; next++) {
                // 获取指定位置上的事件
                Event event = entries[getIndex(next)];
                // 如果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                // 因为ddl事件需要单独返回，因此需要判断entrys中是否应添加了其他事件
                if (ddlIsolation && isDdl(event.getEventType())) {
                    if (entrys.size() == 0) { // 如果entrys中尚未添加任何其他event
                        entrys.add(event);    // 加入当前的DDL事件
                        end = next;           // 更新end为当前值
                    } else {
                        end = next - 1;       // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                    }
                    break;
                } else {
                    entrys.add(event);        // 如果没有开启DDL隔离，直接将事件加入到entrys中
                }
            }
        } else { // 如果batchMode是MEMSIZE
            // 计算本次要获取的event占用最大字节数
            long maxMemSize = batchSize * bufferMemUnit;
            // memsize从0开始，当memsize小于maxMemSize且next未超过maxAbleSequence时，可以进行循环
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {
                // 获取指定位置上的Event
                Event event = entries[getIndex(next)];
                // 如果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                // Get数据时，会通过isDdl方法判断event是否是ddl类型
                if (ddlIsolation && isDdl(event.getEventType())) {
                    if (entrys.size() == 0) { // 如果是ddl隔离，直接返回
                        entrys.add(event);    // 如果没有DML事件，加入当前的DDL事件
                        end = next;           // 更新end为当前
                    } else {
                        end = next - 1;       // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                    }
                    break;
                } else {
                    // 如果没有开启DDL隔离，直接将事件加入到entrys中
                    entrys.add(event);
                    // 并将当前添加的event占用字节数累加到memsize变量上
                    memsize += calculateSize(event);
                    // 记录end位点
                    end = next;
                }
            }
        }

        // =====STEP3 构造PositionRange，表示本次获取的Event的开始和结束位置=====
        /*
        通过CanalEventUtils.createPosition方法计算出第一、最后一个event的位置，作为PostionRange的
        开始和结束。事实上，parser模块解析后，已经将位置信息：binlog文件，position封装到了Event中，
        createPosition方法只是将这些信息提取出来。
         */
        PositionRange<LogPosition> range = new PositionRange<>();
        result.setPositionRange(range);
        // 把entrys列表中的第一个event的位置，当做PositionRange的开始位置
        range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
        // 把entrys列表中的最后一个event的位置，当做PositionRange的结束位置
        range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1)));
        range.setEndSeq(end);

        // =====STEP4 记录一下是否存在可以被ack的点，逆序迭代获取到的Event列表=====
        for (int i = entrys.size() - 1; i >= 0; i--) {
            Event event = entrys.get(i);
            /*
            获取到Event列表后，会从中逆序寻找第一个类型为"事务开始/事务结束/DDL"的Event，将其位置作为PostionRange的可ack位置。
            mysql原生的binlog事件中，总是以一个内容”BEGIN”的QueryEvent作为事务开始，以XidEvent事件表示事务结束。即使我们没有
            显式的开启事务，对于单独的一个更新语句(如Insert、update、delete)，mysql也会默认开启事务。而canal将其转换成更容易
            理解的自定义EventType类型：TRANSACTIONBEGIN、TRANSACTIONEND。
            而将这些事件作为ack点，主要是为了保证事务的完整性。例如client一次拉取了10个binlog event，前5个构成一个事务，后5个
            还不足以构成一个完整事务。在ack后，如果这个client停止了，也就是说下一个事务还没有被完整处理完。尽管之前ack的是10条
            数据，但是client重新启动后，将从第6个event开始消费，而不是从第11个event开始消费，因为第6个event是下一个事务的开始。
            具体逻辑在于，canal server在接受到client ack后，CanalServerWithEmbedded#ack方法会执行。其内部首先根据ack的batchId
            找到对应的PositionRange，再找出其中的ack点，通过CanalMetaManager将这个位置记录下来。之后client重启后，再把这个位
            置信息取出来，从这个位置开始消费。
            也就是说，ack位置实际上提供给CanalMetaManager使用的。而对于MemoryEventStoreWithBuffer本身而言，也需要进行ack，用
            于将已经消费的数据从队列中清除，从而腾出更多的空间存放新的数据。

            StringUtils.isEmpty(event.getGtid())：GTID模式，ack的位点必须是事务结尾，因为下一次订阅的时候mysql会发送这个gtid
            之后的next，如果在事务头就记录了会丢这最后一个事务。
             */
            if ((CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntryType() && StringUtils.isEmpty(event.getGtid()))
                    || CanalEntry.EntryType.TRANSACTIONEND == event.getEntryType()
                    || isDdl(event.getEventType())) {
                // 将事务头/尾设置可被为ack的点，并跳出循环
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
            // 如果没有这三种类型事件，意味着没有可被ack的点
        }

        // =====STEP5 累加getMemSize值=====
        // 通过AtomLong的compareAndSet尝试增加getSequence值
        if (getSequence.compareAndSet(current, end)) {
            // 如果成功，累加getMemSize
            getMemSize.addAndGet(memsize);
            // 如果之前有put操作因为队列满了而被阻塞，这里发送信号，通知队列已经有空位置
            notFull.signal();
            profiling(result.getEvents(), OP.GET);
            return result;
        } else {
            // 如果失败，直接返回空事件列表
            return new Events<>();
        }
    }

    @Override // 获取当前队列中的第一个Event的位置信息
    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            // 没有ack过数据，且队列中有数据
            if (firstSeqeuence == INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                // 没有ack过数据，那么ack为初始值-1，又因为队列中有数据，因此ack+1,即返回队列中第一条数据的位置
                Event event = entries[getIndex(firstSeqeuence + 1)];
                return CanalEventUtils.createPosition(event, false);
            }
            // 已经ack过数据，但是未追上put操作
            else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                // 返回最后一次ack的位置数据 + 1
                Event event = entries[getIndex(firstSeqeuence)];
                return CanalEventUtils.createPosition(event, false);
            }
            // 已经ack过数据，且已经追上put操作，说明队列中所有数据都被消费完了
            else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence == putSequence.get()) {
                // 最后一次ack的位置数据，和last为同一条
                Event event = entries[getIndex(firstSeqeuence)];
                return CanalEventUtils.createPosition(event, false);
            }
            // 没有任何数据，返回null
            else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override // 获取当前队列中的最后一个Event的位置信息
    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SEQUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask];
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SEQUENCE && latestSequence == ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask];
                return CanalEventUtils.createPosition(event, false);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    // ==========================CanalEventStore#ack（确认消费成功）==============================
    @Override // 用于清空指定position之前的数据
    public void ack(Position position) throws CanalStoreException {
        // 更新ackSequence，释放空间给put操作，同时清理对应的数据，帮助GC
        cleanUntil(position, -1L);
    }

    @Override
    public void ack(Position position, Long seqId) throws CanalStoreException {
        cleanUntil(position, seqId);
    }

    @Override
    public void cleanUntil(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    public void cleanUntil(Position position, Long seqId) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long sequence = ackSequence.get();    // 获得当前ack值
            long maxSequence = getSequence.get(); // 获得当前get值

            boolean hasMatch = false;
            long memsize = 0;
            // ack没有list，但有已存在的foreach，还是节省一下list的开销
            long localExecTime = 0L;
            int deltaRows = 0;
            if (seqId > 0) {
                maxSequence = seqId;
            }

            // 迭代所有未被ack的event，从中找出与需要ack的position相同位置的event，清空这个event之前的所有数据。
            // 一旦找到这个event，循环结束。
            for (long next = sequence + 1; next <= maxSequence; next++) {
                // 获得要ack的event
                Event event = entries[getIndex(next)];
                if (localExecTime == 0 && event.getExecuteTime() > 0) {
                    localExecTime = event.getExecuteTime();
                }
                deltaRows += event.getRowsCount();
                // 计算当前要ack的event占用字节数
                memsize += calculateSize(event);
                // 在匹配尚未ack的Event，是否有匹配的位置时，调用了CanalEventUtils#checkPosition方法
                if ((seqId < 0 || next == seqId) && CanalEventUtils.checkPosition(event, (LogPosition) position)) {
                    // 找到对应的position，更新ack seq
                    hasMatch = true;

                    // 如果batchMode是MEMSIZE
                    if (batchMode.isMemSize()) {
                        // 累加ackMemSize
                        ackMemSize.addAndGet(memsize);
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) {
                            entries[getIndex(index)] = null;// 设置为null
                        }

                        // 考虑getFirstPosition/getLastPosition会获取最后一次ack的position信息
                        // ack清理的时候只处理entry=null，释放内存
                        Event lastEvent = entries[getIndex(next)];
                        lastEvent.setEntry(null);
                        lastEvent.setRawEntry(null);
                    }

                    // 累加ack值
                    // 避免并发ack？官方注释说，采用compareAndSet，是为了避免并发ack。我觉得根本不会并发ack，因为都加锁了
                    if (ackSequence.compareAndSet(sequence, next)) {
                        // 如果之前存在put操作因为队列满了而被阻塞，通知其队列有了新空间
                        notFull.signal();
                        ackTableRows.addAndGet(deltaRows);
                        if (localExecTime > 0) {
                            ackExecTime.lazySet(localExecTime);
                        }
                        return;
                    }
                }
            }

            // 找不到对应需要ack的position
            if (!hasMatch) {
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    // ==========================CanalEventStore#rollback==============================
    @Override // 所谓rollback，就是client已经get到的数据，没能消费成功，因此需要进行回滚
    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // 将getSequence的位置重置为ackSequence
            getSequence.set(ackSequence.get());
            // 将getMemSize设置为ackMemSize
            getMemSize.set(ackMemSize.get());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            putSequence.set(INIT_SEQUENCE);
            getSequence.set(INIT_SEQUENCE);
            ackSequence.set(INIT_SEQUENCE);

            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);
            entries = null;
            // for (int i = 0; i < entries.length; i++) {
            // entries[i] = null;
            // }
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================

    /**
     * ==#put操作：检查是否足够的slot空位可供插入==
     *
     * @param sequence 当前putSequence值 + 新插入的event的记录数
     */
    private boolean checkFreeSlotAt(final long sequence) {
        // 用sequence值减去bufferSize
        final long wrapPoint = sequence - bufferSize;
        // 获取get位置跟ack位置中较小的值，事实上，ack位置总是应该小于等于get位置，因此这里总是应该返回的是ack位置
        final long minPoint = getMinimumGetOrAck();
        // 说明二者差值已经超过了bufferSize，不能插入数据，返回false
        if (wrapPoint > minPoint) {
            return false;
        } else {
            // 如果batchMode是MEMSIZE，继续检查是否超出了内存限制
            if (batchMode.isMemSize()) {
                // 使用putMemSize值减去ackMemSize值，得到当前保存的event事件占用的总内存
                final long memsize = putMemSize.get() - ackMemSize.get();
                // 如果没有超出bufferSize * bufferMemUnit内存限制，返回true，否则返回false
                if (memsize < bufferSize * bufferMemUnit) {
                    return true;
                } else {
                    return false;
                }
            } else {
                // 如果batchMode不是MEMSIZE，说明只限制记录数，则直接返回true
                return true;
            }
        }
    }

    /** ==#get操作：检查是否有足够的event可供获取== */
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        // 如果batchMode为ITEMSIZE
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            /*
            第一次订阅之后，需要包含一下start位置，防止丢失第一条记录。
            首先要明确checkUnGetSlotAt方法的startPosition参数到底是从哪里传递过来的？当一个client在获取数据时，
            CanalServerWithEmbedded的getWithoutAck/或get方法会被调用。其内部首先通过CanalMetaManager查找client
            的消费位置信息，由于是第一次，肯定没有记录，因此返回null，此时会调用CanalEventStore的getFirstPosition()
            方法，尝试把第一条数据作为消费的开始。而此时CanalEventStore中可能有数据，也可能没有数据。在没有数据的
            情况下，依然返回null；在有数据的情况下，把第一个Event的位置作为消费开始位置。那么显然，传入checkUnGetSlotAt
            方法的startPosition参数可能是null，也可能不是null。如果不是null的情况下，尽管把第一个event当做开始位置，
            但是因为这个event毕竟还没有消费，所以在消费的时候我们必须也将其包含进去。之所以要+1，因为是第一次获取，
            getSequence的值肯定还是初始值-1，所以要+1变成0之后才是队列的第一个event位置。
             */
            if (startPosition == null || !startPosition.getPostion().isIncluded()) {
                next = next + 1;
            }

            // 先通过current < maxAbleSequence进行一下简单判断，如果不满足，可以直接返回false了
            if (current < maxAbleSequence
                    // 再通过putSequence - getSequence >= batchSize判断是否有足够的数据
                    && next + batchSize - 1 <= maxAbleSequence) {
                return true;
            } else {
                return false;
            }
        } else {
            // 如果batchMode为MEMSIZE
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();
            if (maxAbleSize - currentSize >= batchSize * bufferMemUnit) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * ackSequence总是应该小于等于getSequence，因此这里判断应该是没有必要的
     *
     * github issue：https://github.com/alibaba/canal/issues/966
     */
    private long getMinimumGetOrAck() {
        long get = getSequence.get();
        long ack = ackSequence.get();
        // 返回二者的较小值
        return ack <= get ? ack : get;
    }

    private long calculateSize(Event event) {
        /*
        直接返回binlog中的事件大小，其原理在于：mysql的binlog的event header中，
        都有一个event_length表示这个event占用的字节数
        https://dev.mysql.com/doc/internals/en/event-structure.html

        需要注意的是，这个计算并不精确。原始的event_length表示的是event是二进制
        字节流时的字节数，在转换成java对象后，基本上都会变大
         */
        return event.getRawLength();
    }

    private int getIndex(long sequcnce) {
        // 位运算取余（更高效），
        return (int) sequcnce & indexMask;
        // 等价于: return (int) (int) sequcnce % bufferSize;
    }

    /** Get数据时，判断event是否是ddl类型 */
    private boolean isDdl(EventType type) {
        /*
        这里的EventType是在protocol模块中定义的，并非mysql binlog event结构中的event type。在原始的
        mysql binlog event类型中，有一个QueryEvent，里面记录的是执行的sql语句，Canal通过对这个sql语
        句进行正则表达式匹配，判断出这个event是否是DDL语句，详情参见：SimpleDdlParser#parse方法。
         */
        return type == EventType.ALTER
                || type == EventType.CREATE
                || type == EventType.ERASE
                || type == EventType.RENAME
                || type == EventType.TRUNCATE
                || type == EventType.CINDEX
                || type == EventType.DINDEX;
    }

    private void profiling(List<Event> events, OP op) {
        long localExecTime = 0L;
        int deltaRows = 0;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                if (localExecTime == 0 && e.getExecuteTime() > 0) {
                    localExecTime = e.getExecuteTime();
                }
                deltaRows += e.getRowsCount();
            }
        }
        switch (op) {
            case PUT:
                putTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    putExecTime.lazySet(localExecTime);
                }
                break;
            case GET:
                getTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    getExecTime.lazySet(localExecTime);
                }
                break;
            case ACK:
                ackTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    ackExecTime.lazySet(localExecTime);
                }
                break;
            default:
                break;
        }
    }

    private enum OP {
        PUT, GET, ACK
    }

    // ================ setter / getter ==================
    public int getBufferSize() {
        return this.bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public AtomicLong getPutSequence() {
        return putSequence;
    }

    public AtomicLong getAckSequence() {
        return ackSequence;
    }

    public AtomicLong getPutMemSize() {
        return putMemSize;
    }

    public AtomicLong getAckMemSize() {
        return ackMemSize;
    }

    public BatchMode getBatchMode() {
        return batchMode;
    }

    public AtomicLong getPutExecTime() {
        return putExecTime;
    }

    public AtomicLong getGetExecTime() {
        return getExecTime;
    }

    public AtomicLong getAckExecTime() {
        return ackExecTime;
    }

    public AtomicLong getPutTableRows() {
        return putTableRows;
    }

    public AtomicLong getGetTableRows() {
        return getTableRows;
    }

    public AtomicLong getAckTableRows() {
        return ackTableRows;
    }

}
