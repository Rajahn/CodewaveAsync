package edu.vt.ranhuo.asyncmaster.context;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.enums.Status;
import edu.vt.ranhuo.asynccore.service.leader.LeaderService;
import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImpl;
import edu.vt.ranhuo.asynccore.service.task.TaskService;
import edu.vt.ranhuo.asynccore.service.task.impl.TaskServiceImpl;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Slf4j
public class Master implements IMaster<String> {
    private final TaskContext context;
    private final LeaderService leaderService;
    private final TaskService<String, String> service;

    public Master(TaskConfig config) {
        this.context = new TaskContext(config);
        this.service = new TaskServiceImpl(context);
        this.leaderService = new LeaderServiceImpl(context, context.masterHashKey());
    }

    @Override
    public void send(QueueType queue, double score, String value) {
        context.getRedissonUtils().zadd(context.getQueue(queue), score, value);
        log.info("master[{}] send finished, queue: {}, score: {}, value: {}", context.masterHashKey(), queue, score, value);
    }
    //如果成功获取到锁，它会尝试从resultQueue队列中弹出一个元素。这个元素是一个Optional<String>类型，可能包含一个字符串，也可能为空。
    //如果成功弹出一个元素（即Optional<String>不为空），它会将这个元素发送到执行队列，并从resultQueue队列中移除这个元素。
    //多个依赖相同config创建的master实例抢占同一个锁，redisson的lock方法无论是否上锁成功都会解锁
    @Override
    public Optional<String> consume() {
        return context.getRedissonUtils().lock(context.masterConsumerLock(), Optional.empty(), (t) -> {
            Optional<String> rValue = context.getRedissonUtils().rlpop(context.resultQueue());
            rValue.ifPresent(v -> {
                service.sendExecuteQueue(context.masterHashKey(), rValue.get());
                context.getRedissonUtils().lpop(context.resultQueue());
            });
            log.info("master[{}] consume finished, queue: {}, value: {}", context.masterHashKey(), context.resultQueue(), rValue);
            return rValue;
        });
    }

    @Override
    public Status delete(QueueType queue, String value) {
        return context.getRedissonUtils().zrem(context.getQueue(queue), value) == TRUE
                ? Status.SUCCESS : Status.NON_EXISTENT;
    }

    @Override
    public Status delete(String value) {
        for (QueueType queueType : context.getAllQueueType()) {
            if (delete(queueType, value) == Status.SUCCESS) {
                return Status.SUCCESS;
            }
        }
        return Status.NON_EXISTENT;
    }

    @Override
    public Collection<RedissonUtils.ScoredEntryEx<String>> getQueue(QueueType queue) {
        return context.getRedissonUtils().zrangebyscore(context.getQueue(queue), ZERO, END_INDEX);
    }

    @Override
    public Map<QueueType, Collection<RedissonUtils.ScoredEntryEx<String>>> getAllQueue() {
        return new HashMap<QueueType, Collection<RedissonUtils.ScoredEntryEx<String>>>() {{
            context.getAllQueueType().forEach(queue -> put(queue,
                    context.getRedissonUtils().zrangebyscore(context.getQueue(queue), ZERO, END_INDEX)));
        }};
    }

    @Override
    public Optional<List<String>> getExecuteQueue() {
        Optional<String> value = context.getRedissonUtils().hget(context.executeHash(), context.masterHashKey());
        return value.map(context::deleteSplit);
    }

    @Override
    public int getExecuteQueueSum() {
        return service.getExecuteQueueSum(context.masterHashKey());
    }

    @Override
    public int getResultQueueSum() {
        return context.getRedissonUtils().llen(context.resultQueue());
    }

    @Override
    public double getQueueMax(QueueType queue) {
        return context.getRedissonUtils().zmax(context.getQueue(queue));
    }

    @Override
    public int getQueueSize(QueueType queue) {
        return context.getRedissonUtils().zcard(context.getQueue(queue));
    }

    @Override
    public Map<QueueType, Integer> getQueueSize() {
        return new HashMap<QueueType, Integer>() {{
            context.getAllQueueType().forEach(queue -> put(queue, getQueueSize(queue)));
        }};
    }

    @Override
    public void commit(String resultValue) {
        service.commitExecuteTask(context.masterHashKey(), resultValue);
        log.info("master[{}] commit finished, executeHash: {}, resultValue: {}", context.masterHashKey(),
                context.executeHash(), resultValue);
    }

    @Override
    public String getNodeInfo() {
        return context.masterHashKey();
    }

    @Override
    public Set<String> getActiveNodeInfo() {
        return leaderService.getActiveNodeInfo();
    }

    @Override
    public Optional<String> getLeaderInfo() {
        return leaderService.getLeaderInfo();
    }

    @Override
    public void close() {
        this.leaderService.close();
    }
}
