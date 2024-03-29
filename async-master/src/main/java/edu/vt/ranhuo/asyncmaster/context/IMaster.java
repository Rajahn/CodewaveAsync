package edu.vt.ranhuo.asyncmaster.context;

import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.enums.Status;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;

import java.io.Closeable;
import java.util.*;

public interface IMaster<T> extends Closeable {
    /**
     * 将任务循环投放至全部队列中
     */
    void send(double score, T value);


    /**
     * 将任务存放至队列中
     * 注意: 由于底层使用的是zset, 故存入的任务字符串要保持唯一
     */
    void send(QueueType queue, double score, T value);

    /**
     * 消费resultQueue中数据, 此函数并不控制消费速度, 应由用户业务控制消费速度, 若列表无数据则返回空Optional
     * 注意: 此接口实现要兼容分布式服务, 需要分布式锁
     */
    Optional<T> consume();

    /**
     * 在指定队列中查找并删除任务, 此函数只会删除等待队列中的任务, 若slave节点已经获取到任务则无法删除, 并且在consume函数中依旧可以获取到删除任务的返回结果
     * 建议用户在业务层面控制, 例如: 将删除的任务在数据库设置为failed状态, 当consume获取删除任务后判断数据库任务状态如果为failed则无需处理结果;
     */
    Status delete(QueueType queue, T value);

    /**
     * 在所有待执行队列中查找并删除此任务, 其余同上
     *
     * @param value 任务数据
     * @return 结果状态
     */
    Status delete(T value);

    /**
     * 获取指定队列列表
     *
     * @param queue
     * @return
     */
    Collection<RedissonUtils.ScoredEntryEx<T>> getQueue(QueueType queue);

    /**
     * 获取所有队列列表
     *
     * @return
     */
    Map<QueueType, Collection<RedissonUtils.ScoredEntryEx<T>>> getAllQueue();

    /**
     * 获取执行队列集合
     *
     * @return
     */
    Optional<List<T>> getExecuteQueue();

    /**
     * 获取执行队列任务数
     *
     * @return
     */
    int getExecuteQueueSum();

    /**
     * 获取结果队列任务数
     *
     * @return
     */
    int getResultQueueSum();

    /**
     * 获取指定队列的最高优先级
     *
     * @return
     */
    double getQueueMax(QueueType queue);

    /**
     * 获取指定任务队列任务数量
     *
     * @return
     */
    int getQueueSize(QueueType queue);

    /**
     * 获取所有任务队列任务数量
     *
     * @return
     */
    Map<QueueType, Integer> getQueueSize();

    /**
     * 结束result任务, 此操作将删除Master执行副本中的元数据, 确保业务层面处理成功后调动此函数
     *
     * @return
     */
    void commit(T resultValue);

    /**
     * 获取当前节点信息
     */
    String getNodeInfo();

    /**
     * 获取所有活跃节点
     */
    Set<String> getActiveNodeInfo();

    /**
     * 获取Leader信息, 若leader不存在, 则无法进行生产消费
     */
    Optional<String> getLeaderInfo();

    /**
     * 关闭接口, 此接口只会关闭master节点的心跳发送和leader竞争, 关闭后用户依然可以调用send||consume接口
     */
    void close();

}
