package edu.vt.ranhuo.asynccore.service.task.impl;

import edu.vt.ranhuo.asynccore.config.ITaskContext;
import edu.vt.ranhuo.asynccore.service.task.TaskService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.redisson.api.RTransaction;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.HASH_VALUE_SPLIT;
import static edu.vt.ranhuo.asynccore.enums.CommonConstants.ZERO;

@Slf4j
public class TaskServiceImpl implements TaskService<String, String> {

    private final ITaskContext context;

    public TaskServiceImpl(ITaskContext context) {
        this.context = context;
    }

    @Override
    public void sendExecuteQueue(String hashKey, String value) {
        Optional<String> oldHashValue = context.getRedissonUtils().hget(context.executeHash(), hashKey);
        String hashValue = context.addSplit(oldHashValue, value);
        context.getRedissonUtils().hset(context.executeHash(), hashKey, hashValue);
        log.debug("TaskService:sendExecuteQueue, executeHash: {}, hashKey: {}, value: {}, oldHashValue: {}, newHashValue: {}",
                context.executeHash(), hashKey, value, oldHashValue, hashValue);
    }

    public void sendExecuteQueue(RTransaction transaction,String hashKey, String value) {
        Optional<String> oldHashValue = context.getRedissonUtils().hget(context.executeHash(), hashKey);
        String hashValue = context.addSplit(oldHashValue, value);
        transaction.getMap(context.executeHash()).fastPut(hashKey, hashValue);
        log.debug("TaskService:sendExecuteQueue, executeHash: {}, hashKey: {}, value: {}, oldHashValue: {}, newHashValue: {}",
                context.executeHash(), hashKey, value, oldHashValue, hashValue);
    }

    @Override
    public void commitExecuteTask(String hashKey, String value) {
        Optional<String> oldHashValue = context.getRedissonUtils().hget(context.executeHash(), hashKey);
        oldHashValue.ifPresent((v) -> {
            List<String> collect = context.deleteSplit(v).stream().filter((s) -> !s.equals(value)).collect(Collectors.toList());
            String hashValue = null;
            if (collect.isEmpty()) {
                context.getRedissonUtils().hdel(context.executeHash(), hashKey);
            } else {
                hashValue = String.join(HASH_VALUE_SPLIT, collect);
                context.getRedissonUtils().hset(context.executeHash(), hashKey, hashValue);
            }
            log.debug("[{}] commitExecuteTask, executeHash: {}, hashKey: {}, value: {}, oldHashValue: {}, newHashValue: {}",
                    hashKey, context.executeHash(), hashKey, value, oldHashValue, hashValue);
        });
    }

    @Override
    public int getExecuteQueueSum(String hashKey) {
        Optional<String> execute = context.getRedissonUtils().hget(context.executeHash(), hashKey);
        return execute.map(s -> context.deleteSplit(s).size()).orElse(ZERO);
    }
}
