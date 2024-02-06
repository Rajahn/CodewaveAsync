package edu.vt.ranhuo.asynctest;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.lambda.ProcessLambda;
import edu.vt.ranhuo.asyncmaster.context.IMaster;
import edu.vt.ranhuo.asyncmaster.context.Master;
import edu.vt.ranhuo.asyncslave.context.ISlave;
import edu.vt.ranhuo.asyncslave.context.Slave;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Slf4j
public class asynctest {
    /**
     *  整体测试分布式环境下表现，双master、双slave，观察日志，Redis队列中的数据是否符合预期
     *   1. masterFirst: master启动，成为leader,循环生产、消费, 100s后宕机，此时leader诞生，130秒后判断firstMaster节点宕机
     *   执行宕机处理逻辑
     *   2. masterSecond: master先启动，master循环生产、消费
     *   3. slaveFirst: slave启动，循环消费，200s后宕机，230s后被判断为宕机，执行宕机处理逻辑
     *   4. slaveSecond: slave启动，循环消费
     *   5. 300s后所有节点停止运行，观察剩余数据状态，是否符合预期，执行clean清除
     */
    private final AtomicBoolean stopper = new AtomicBoolean();
    private final Random rand = new Random();
    private final String prefix = "test:codewave";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private TaskContext context;
    private TaskConfig config;
    private IMaster<String> master;
    private ISlave<String> slave;

    @Before
    public void setUp() throws Exception {
        final URL resource = asynctest.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        config = TaskConfig.builder().prefix(prefix).redissonClient(redissonClient)
                .heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        log.info(String.format("init config: %s", config));
        context = new TaskContext(config);
    }

    @After
    public void tearDown() {
        close();
    }

    private void close() {
        this.stopper.set(TRUE);
        if (Objects.nonNull(master)) {
            master.close();
        }
        if (Objects.nonNull(slave)) {
            slave.close();
        }
        log.info("service closed");
    }


    @Test
    public void masterFirst() {
        master = new Master(config);
        startMaster(MASTER_PREFIX.concat(REDIS_SPLIT).concat(String.valueOf(FIRST)));
        waiting(heartbeatInterval * TENTH); // 100s后停止
        log.info("MasterFirst:getExecuteQueue: {}", master.getExecuteQueue());
    }

    @Test
    public void masterSecond() {
        master = new Master(config);
        startMaster(MASTER_PREFIX.concat(REDIS_SPLIT).concat(String.valueOf(SECOND)));
        waiting(heartbeatInterval * EXPIRATION_COUNT * SECOND); // 200s后停止
        log.info("MasterFirst:getExecuteQueue: {}", master.getExecuteQueue());
    }

    @Test
    public void slaveFirst() {
        slave = new Slave(config);
        startSlave(SLAVE_PREFIX.concat(REDIS_SPLIT).concat(String.valueOf(FIRST)));
        waiting(heartbeatInterval * EXPIRATION_COUNT * THIRD); // 300s后停止
    }

    @Test
    public void slaveSecond() {
        slave = new Slave(config);
        startSlave(SLAVE_PREFIX.concat(REDIS_SPLIT).concat(String.valueOf(SECOND)));
        waiting(heartbeatInterval * EXPIRATION_COUNT * FOURTH); // 400s后停止, 消费结束
    }

    private void startMaster(String name) {
        new Thread(this::masterSend, name).start();
        new Thread(this::masterConsume, name).start();
    }

    private void startSlave(String name) {
        new Thread(this::slaveConsume, name).start();
    }

    // master循环生产
    private void masterSend() {
        List<QueueType> allQueueType = context.getAllQueueType();
        process(() -> {
            QueueType queueType = allQueueType.get(rand.nextInt(allQueueType.size()));
            int score = rand.nextInt(FIFTH);
            String value = Thread.currentThread().getName().concat(REDIS_SPLIT).concat(uuid()).concat(String.valueOf(score * MILLISECOND));
            master.send(queueType, score, value);
        });
    }

    // master循环消费
    private void masterConsume() {
        process(() -> {
            Optional<String> consume = master.consume();
            waiting(heartbeatInterval); // 故意延迟master消费结果队列中的数据，为了当此节点宕机时执行队列有数据，可以完整触发leader宕机处理
            consume.ifPresent((v) -> master.commit(v));
        });
    }

    // slave循环消费
    private void slaveConsume() {
        process(() -> {
            Optional<String> consume = slave.consume();
            waiting(heartbeatInterval); // 故意延迟slave消费队列中的数据，为了当此节点宕机时执行队列有数据，可以完整触发leader宕机处理
            consume.ifPresent((v) -> slave.commit(v.concat(REDIS_SPLIT).concat(Thread.currentThread().getName()), v));
        });
    }

    public void process(ProcessLambda lambda) {
        while (!stopper.get()) {
            lambda.process();
            waiting(heartbeatInterval);
        }
    }

    private void waiting(long interval) {
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            log.error("thread sleep error!", e);
        }
    }

    private String uuid() {
        return UUID.randomUUID().toString().substring(ZERO, SEVENTH);
    }

    @Test
    public void clean() {
        context.getAllKey().forEach((v) -> context.getRedissonUtils().del(v));
    }

}
