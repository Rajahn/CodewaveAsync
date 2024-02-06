package edu.vt.ranhuo.asyncslave.context;

import edu.vt.ranhuo.asynccore.config.TaskConfig;
import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.service.leader.LeaderService;
import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImpl;
import edu.vt.ranhuo.asynccore.service.task.TaskService;
import edu.vt.ranhuo.asynccore.service.task.impl.TaskServiceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

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
    @Override
    public Optional<String> consume() {
        return context.getRedissonUtils().lock(context.slaveConsumerLock(), Optional.empty(), (t) -> {
            for (String queueName : context.getAllQueue()) {
                Optional<String> tValue = context.getRedissonUtils().zrpop(queueName);
                if (tValue.isPresent()) {
                    service.sendExecuteQueue(context.slaveHashKey(), tValue.get());
                    context.getRedissonUtils().zrem(queueName, tValue.get());
                    log.info("slave[{}] consume finished, queue: {}, value: {}", context.slaveHashKey(), queueName, tValue);
                    return tValue;
                }
            }
            return Optional.empty();
        });
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
