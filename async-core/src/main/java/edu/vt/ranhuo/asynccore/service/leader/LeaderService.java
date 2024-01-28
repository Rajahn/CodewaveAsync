package edu.vt.ranhuo.asynccore.service.leader;

import edu.vt.ranhuo.asynccore.inspect.Initialize;

import java.io.Closeable;
import java.util.Optional;
import java.util.Set;

public interface LeaderService extends Initialize, Closeable {
    /**
     * 心跳接口
     */
    void heart();

    /**
     * leader抢占接口
     */
    void seize();

    /**
     * 节点监听接口
     */
    void listen();

    /**
     * 获取所有活跃节点
     */
    Set<String> getActiveNodeInfo();

    /**
     * 获取Leader信息
     */
    Optional<String> getLeaderInfo();

    /**
     * 关闭接口
     */
    void close();
}
