package edu.vt.ranhuo.asyncslave.context;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.service.leader.LeaderService;
import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImpl;
import edu.vt.ranhuo.asynccore.service.task.TaskService;
import edu.vt.ranhuo.asynccore.service.task.impl.TaskServiceImpl;
import edu.vt.ranhuo.asynccore.utils.QueueSelector;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class Slave implements ISlave<String> {
    private final TaskContext context;
    private final TaskService<String, String> service;
    private final LeaderService leaderService;

    public Slave(TaskConfig config) {
        this.context = new TaskContext(config);
        this.service = new TaskServiceImpl(context);
        this.leaderService = new LeaderServiceImpl(context, context.slaveHashKey());
    }

    //对于每一个队列，这个方法会尝试从队列的右端弹出一个元素，这个操作由context.getRedissonUtils().zrpop(queueName)完成。
    //如果弹出的元素存在（即Optional<String>不为空），这个方法会执行以下操作：
    //将这个元素添加到执行队列中，这个操作由service.sendExecuteQueue(context.slaveHashKey(), tValue.get())完成。
    //从原队列中删除这个元素，这个操作由context.getRedissonUtils().zrem(queueName, tValue.get())完成
    // TODO: lua脚本优化 取任务-存入执行队列的过程
    @Override
    public Optional<String> consume() {
        List<String> allQueue = context.getAllQueue();
        Collections.shuffle(allQueue);
        for (String queueName : allQueue) {
            // 为每个队列名称获取一个唯一的锁
            String lockKey = context.slaveConsumerLock(queueName);
            // 尝试获取锁并消费队列
            Optional<String> lockedConsumeResult = context.getRedissonUtils().lock(lockKey, Optional.empty(), (t) -> {
                log.info("slave[{}] consume start, queue: {}", context.slaveHashKey(), queueName);
                Optional<String> tValue = context.getRedissonUtils().zrpop(queueName);
                if (tValue.isPresent()) {
                    service.sendExecuteQueue(context.slaveHashKey(), tValue.get());
                    context.getRedissonUtils().zrem(queueName, tValue.get());
                    log.info("slave[{}] consume finished, queue: {}, value: {}", context.slaveHashKey(), queueName, tValue);
                    return tValue;
                }
                return Optional.empty();
            });
            // 如果当前队列成功消费了任务，则结束循环返回结果
            if (lockedConsumeResult.isPresent()) {
                return lockedConsumeResult;
            }
        }
        return Optional.empty(); // 如果所有队列都没有任务，返回空的Optional
    }

    public Optional<String> consume_unlock() {
        // todo 在队列数比worker数少的情况下，需要加锁获取任务
        List<Integer> queuesForWorker = leaderService.getQueuesForWorker(context.slaveHashKey());
        if(queuesForWorker.size()== 0){
            return Optional.empty();
        }
        List<String> allQueue =  new ArrayList<>();
        for (int i = 0; i < queuesForWorker.size(); i++) {
            QueueType queue = QueueSelector.mapIntToQueueType(queuesForWorker.get(i));
            String queueName = context.getQueue(queue);
            allQueue.add(queueName);
        }
        Collections.shuffle(allQueue);

        for (String queueName : allQueue) {
            Optional<String> consumeResult = context.getRedissonUtils().zrpop(queueName);
            if (consumeResult.isPresent()) {
                log.info("slave[{}] consume start, queue: {}", context.slaveHashKey(), queueName);
                service.sendExecuteQueue(context.slaveHashKey(), consumeResult.get());
                log.info("slave[{}] consume finished, queue: {}, value: {}", context.slaveHashKey(), queueName, consumeResult);
                return consumeResult;
            }
        }
        return Optional.empty(); // 如果所有队列都没有任务，返回空的Optional
    }

    @Override
    public int getExecuteQueueSum() {
        return service.getExecuteQueueSum(context.slaveHashKey());
    }

    @Override
    public void commit(String value, String executeValue) {
        context.getRedissonUtils().rpush(context.resultQueue(), value);
        service.commitExecuteTask(context.slaveHashKey(), executeValue);
        log.info("slave[{}] commit finished, resultQueue: {}, value: {}", context.slaveHashKey(), context.resultQueue(), value);
    }

    @Override
    public String getNodeInfo() {
        return context.slaveHashKey();
    }

    @Override
    public void close() {
        this.leaderService.close();
    }
}
