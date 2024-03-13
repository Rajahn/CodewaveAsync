package edu.vt.ranhuo.asyncslave.context;

import java.io.Closeable;
import java.util.Optional;

public interface ISlave<T> extends Closeable {
    /**
     * 每次调用消费Queue中数据, 若列表无数据则返回空Optional
     * 注意: 因为是轮流从多个队列中获取数据, 所以需要使用分布式锁
     */
    Optional<T> consume();
    /**
     * 无锁消费，获取当前节点所负责的队列中的任务，但当当前节点负责的队列数多于1 时，采用consume()方法
     */
    Optional<T> consume_unlock();

    /**
     * 获取正在执行的队列任务数
     *
     * @return
     */
    int getExecuteQueueSum();

    /**
     * 结束执行任务, 将任务结果value存放至result队列中, 并删除执行队列中的任务executeValue
     * executeValue的值是consume函数获取的数据
     *
     * @param value 任务结果
     * @param executeValue  删除执行中的任务
     */
    void commit(T value, T executeValue);

    /**
     * 获取当前节点信息
     */
    String getNodeInfo();

    /**
     * 关闭接口
     */
    void close();

}
