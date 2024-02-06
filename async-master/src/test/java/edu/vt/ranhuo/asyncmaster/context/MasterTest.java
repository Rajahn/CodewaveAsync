package edu.vt.ranhuo.asyncmaster.context;

import java.net.URL;
import java.util.stream.IntStream;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.enums.CommonConstants;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;
import static java.util.Calendar.SECOND;

@Slf4j
public class MasterTest {
    private final String prefix = "test:master";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private IMaster<String> master;
    private TaskContext context;

    @Before
    public void setUp() throws Exception {
        final URL resource = MasterTest.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        final TaskConfig config = TaskConfig.builder().prefix(prefix).redissonClient(redissonClient)
                .heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        log.info(String.format("init config: %s", config));
        context = new TaskContext(config);
        master = new Master(config);
    }

    @After
    public void tearDown() {
        close();
       // clean();
    }

    @Test
    public void close() {
        master.close();
        log.info("Master closed");
    }

    @Test
    public void clean() {
        context.getAllKey().forEach((v) -> context.getRedissonUtils().del(v));
    }

    @Test
    public void testBegin(){
        getNodeInfo();
        getActiveNodeInfo();
        getLeaderInfo();
        send();
        getQueue();
        delete();
        getQueueMax();
        getQueueSize();
        setResultQueue(); // 结果队列各插入两条数据
        getResultQueueSum();
        consume();
        getExecuteQueue();
        commit();
    }

    @Test
    public void send() {
        IntStream.range(ZERO, SECOND).forEach(weight -> context.getAllQueueType().forEach(queue -> // 三种优先级队列各插入两条数据
                master.send(queue, weight, String.valueOf(queue).concat(String.valueOf(weight)))));
        log.info("send finished");
    }

    @Test
    public void consume() {
        IntStream.range(ZERO, THIRD).forEach((v) -> log.info("consume: {}", master.consume())); // THIRD多消费一次
        log.info("consume finished, executeHash: {}", context.getRedissonUtils().hgetall(context.executeHash()));
    }

    @Test
    public void delete() {
        final String value = String.valueOf(QueueType.HIGN).concat(String.valueOf(FIRST));
        log.info("delete value:{} finished, boolean: {}", value, master.delete(QueueType.HIGN, value));
    }

    @Test
    public void getQueue() {
        context.getAllQueueType().forEach(queue -> log.info("getQueue finished, {}:{}", queue, master.getQueue(queue)));
    }

    @Test
    public void getExecuteQueue() {
        log.info("getExecuteQueue finished, {}", master.getExecuteQueue());
    }

    @Test
    public void getResultQueueSum() {
        log.info("getResultQueueSum finished, {}", master.getResultQueueSum());
    }

    @Test
    public void getQueueMax() {
        context.getAllQueueType().forEach(queue -> log.info("getQueueMax finished, queue: {}, max: {}", queue, master.getQueueMax(queue)));
    }

    @Test
    public void getQueueSize() {
        context.getAllQueueType().forEach(queue -> log.info("getQueueSum finished, queue: {}, sum: {}", queue, master.getQueueSize(queue)));
    }

    @Test
    public void commit() {
        IntStream.range(ZERO, THIRD).forEach((number) ->
                master.commit(context.resultQueue().concat(REDIS_SPLIT).concat(String.valueOf(number)))); // THIRD多提交一次
        log.info("commit finished, ExecuteQueue: {}", master.getExecuteQueue());
    }

    @Test
    public void getNodeInfo() {
        log.info("getNodeInfo finished, info: {}", master.getNodeInfo());
    }

    @Test
    public void getActiveNodeInfo() {
        log.info("getActiveNodeInfo finished, activeInfo: {}", master.getActiveNodeInfo());
    }

    @Test
    public void getLeaderInfo() {
        log.info("getLeaderInfo finished, leaderInfo: {}", master.getLeaderInfo());
    }

    private void setResultQueue() {
        IntStream.range(ZERO, SECOND).forEach(number ->
                context.getRedissonUtils().rpush(context.resultQueue(), context.resultQueue().concat(REDIS_SPLIT).concat(String.valueOf(number))));
    }



}