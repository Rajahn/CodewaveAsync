package edu.vt.ranhuo.asyncslave.context;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.net.URL;
import java.util.stream.IntStream;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Slf4j
public class SlaveTest {
    private final String prefix = "test:slave";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private ISlave<String> slave;
    private TaskContext context;

    @Before
    public void setUp() throws Exception {
        final URL resource = SlaveTest.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        final TaskConfig config = TaskConfig.builder().prefix(prefix).redissonClient(redissonClient).heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        log.info(String.format("init config: %s", config));
        context = new TaskContext(config);
        slave = new Slave(config);
    }

    @After
    public void tearDown() {
        clean();
        close();
    }

    @Test
    public void all() throws InterruptedException {
        setQueueTask();
        getNodeInfo();
        consume();
        getExecuteQueueSum();
        commit();
        getExecuteQueueSum();
        Thread.sleep(100000);
    }

    @Test
    public void consume() {
        IntStream.range(ZERO, SEVENTH).forEach((v) -> log.info("consume{}: {}", v, slave.consume()));
        log.info("consume finished, executeHash: {}", context.getRedissonUtils().hgetall(context.executeHash()));
    }

    @Test
    public void getExecuteQueueSum() {
        log.info("getExecuteQueueSum finished: {}", slave.getExecuteQueueSum());
    }

    @Test
    public void commit() {
        IntStream.range(ZERO, SECOND).forEach(number -> context.getAllQueue().forEach((queue) -> {
            String value = queue.concat(REDIS_SPLIT).concat(String.valueOf(number));
            slave.commit(value, value);
        }));
    }

    @Test
    public void getNodeInfo() {
        log.info("getNodeInfo finished, info: {}", slave.getNodeInfo());
    }

    @Test
    public void close() {
        slave.close();
    }

    // 三种优先级队列各插入两条数据
    private void setQueueTask() {
        IntStream.range(ZERO, SECOND).forEach(number ->
                context.getAllQueue().forEach((queue) -> context.getRedissonUtils().zadd(queue, number, queue.concat(REDIS_SPLIT).concat(String.valueOf(number)))));
    }

    @Test
    public void clean() {
        context.getAllKey().forEach((v) -> context.getRedissonUtils().del(v));
    }

}