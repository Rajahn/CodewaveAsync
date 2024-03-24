package edu.vt.ranhuo.asynccore.service.task;

import org.redisson.api.RTransaction;

public interface TaskService<K,V> {
    /**
     * 将数据存储放至执行队列
     *
     * @param hashKey 执行中的hashKey
     * @param t 任务
     */
    void sendExecuteQueue(K hashKey, V t);

    void sendExecuteQueue(RTransaction transaction, K hashKey, V t);

    /**
     * 删除执行队列中的任务
     *
     * @param hashKey 执行中的hashKey
     * @param t 任务
     */
    void commitExecuteTask(K hashKey, V t);

    /**
     * 获取正在执行的队列任务数
     *
     * @param hashKey 执行中的hashKey
     * @return  正在执行的任务数
     */
    int getExecuteQueueSum(K hashKey);
}
