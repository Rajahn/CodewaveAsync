package edu.vt.ranhuo.asynccore.service.task.impl;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImpl;
import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImplTest;
import edu.vt.ranhuo.asynccore.service.task.TaskService;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.net.URL;
import java.util.stream.IntStream;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.SECOND;
import static edu.vt.ranhuo.asynccore.enums.CommonConstants.ZERO;

@Slf4j
public class TaskServiceImplTest {
    private final String prefix = "test:codewave:service";
    private final long heartbeatInterval = 10 * 1000;
    private final int expirationCount = 3;
    private TaskContext context;
    private TaskService<String, String> service;

    @Before
    public void setUp() throws Exception {
        final URL resource = LeaderServiceImplTest.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        final TaskConfig config =
                TaskConfig.builder().prefix(prefix).redissonClient(redissonClient).heartbeatInterval(heartbeatInterval).expirationCount(expirationCount).build();
        log.info(String.format("init config: %s", config));
        context = new TaskContext(config);
        service = new TaskServiceImpl(context);
    }

    //@After
    public void tearDown() {
        clean();
    }

    @Test
    public void sendExecuteQueue() {
        IntStream.range(ZERO, SECOND).forEach((v) -> service.sendExecuteQueue(context.masterHashKey(), String.valueOf(v)));
        log.info("sendExecuteQueue finished, executeHash: {}, hashKey: {}, value: {}", context.executeHash(),
                context.masterHashKey(), context.getRedissonUtils().hget(context.executeHash(), context.masterHashKey()));
    }

    @Test
    public void commitExecuteTask() {
        sendExecuteQueue();
        IntStream.range(ZERO, SECOND).forEach((v) -> service.commitExecuteTask(context.masterHashKey(), String.valueOf(v)));
        log.info("commitExecuteTask finished, executeHash: {}, hashKey: {}, value: {}", context.executeHash(),
                context.masterHashKey(), context.getRedissonUtils().hget(context.executeHash(), context.masterHashKey()));
    }

    @Test
    public void clean() {
        context.getAllKey().forEach((v) -> context.getRedissonUtils().del(v));
    }
}