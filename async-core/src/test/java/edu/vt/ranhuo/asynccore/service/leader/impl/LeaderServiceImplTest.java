package edu.vt.ranhuo.asynccore.service.leader.impl;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.service.leader.LeaderService;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.net.URL;
import java.util.Optional;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Slf4j
public class LeaderServiceImplTest  {

    private final String prefix = "test:leader";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private final long leaderSleep = 1000;
    private final long sleep = 1000 * 60 * 60;
    private TaskContext context;
    @Before
    public void setUp() throws Exception {
        final URL resource = LeaderServiceImplTest.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        final TaskConfig config =
                TaskConfig.builder().prefix(prefix).redissonClient(redissonClient).heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        log.info(String.format("init config: %s", config));
        context = new TaskContext(config);
    }
    @After
    public void tearDown() throws Exception {
        context.getAllKey().forEach((v) -> context.getRedissonUtils().del(v));
    }

    @Test
    public void testSlaveShutdown() throws InterruptedException {
        setSlaveExecute();
        LeaderService leaderServiceM = new LeaderServiceImpl(context, context.masterHashKey());
        Thread.sleep(leaderSleep);
        LeaderService leaderServiceS = new LeaderServiceImpl(context, context.slaveHashKey());
        Thread.sleep(heartbeatInterval * SECOND);
        // slave.shutdown, 等待Master逻辑处理, 30s后高优队列hignQueue 会有数据
        leaderServiceS.close();
        Thread.sleep(heartbeatInterval * expirationCount * SECOND);
        log.info("hignQueue: {}", String.join(REDIS_SPLIT, context.getRedissonUtils().zrange(context.getQueue(QueueType.ONE), ZERO, END_INDEX)));
        Thread.sleep(5000);
    }

    @Test
    public void testMasterShutdown() throws InterruptedException {
        setMasterExecute();
        LeaderService leaderServiceM = new LeaderServiceImpl(context, context.masterHashKey());
        Thread.sleep(leaderSleep);
        LeaderService leaderServiceS = new LeaderServiceImpl(context, context.slaveHashKey());
        Thread.sleep(heartbeatInterval * SECOND);
        // master.shutdown, slave晋升leader, 等待slave逻辑处理, 30s后高优队列resultQueue 会有数据
        leaderServiceM.close();
        Thread.sleep(heartbeatInterval * expirationCount * SECOND);
        log.info("firstQueue: {}", String.join(REDIS_SPLIT, context.getRedissonUtils().zrange(context.getQueue(QueueType.ONE), ZERO, END_INDEX)));
        Thread.sleep(sleep);
    }

    private void setSlaveExecute() {
        String value = context.addSplit(Optional.of("task_second"), "task_first");
        context.getRedissonUtils().hset(context.executeHash(), context.slaveHashKey(), value);
        log.info(String.format("setExecute finished, hashExecute value: %s", value));
    }


    private void setMasterExecute() {
        String value = context.addSplit(Optional.of("task_second"), "task_first");
        context.getRedissonUtils().hset(context.executeHash(), context.masterHashKey(), value);
        log.info(String.format("setExecute finished, hashExecute value: %s", value));
    }
}