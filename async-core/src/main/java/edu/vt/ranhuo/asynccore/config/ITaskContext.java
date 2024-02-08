package edu.vt.ranhuo.asynccore.config;

import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;

import java.util.List;
import java.util.Optional;

public interface ITaskContext {
    String prefix();

    RedissonUtils getRedissonUtils();

    long heartbeatInterval();

    int expirationCount();

    long timestamp();

    long expirationTime();

    String leaderLock();

    String leaderName();

    String getQueue(QueueType queueType);

    List<String> getAllQueue();

    List<QueueType> getAllQueueType();

    String resultQueue();

    String masterConsumerLock();

    String slaveConsumerLock(String queue);

    String executeHash();

    String heartHash();

    String masterHashKey();

    String slaveHashKey();

    List<String> getAllKey();

    String addSplit(Optional<String> source, String value);

    List<String> deleteSplit(String value);
}
