package edu.vt.ranhuo.asynccore.config;

import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import lombok.Getter;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Getter
public class TaskContext implements ITaskContext {

    private final TaskConfig config;
    private final String pidHostname;
    private final RedissonUtils redissonUtils;

    public TaskContext(TaskConfig config) {
        this.config = config;
        this.pidHostname = ManagementFactory.getRuntimeMXBean().getName();
        this.redissonUtils = RedissonUtils.getInstance(Optional.ofNullable(config.getRedissonClient()));
    }

    @Override
    public String prefix() {
        return this.config.getPrefix();
    }

    @Override
    public RedissonUtils getRedissonUtils() {
        return this.redissonUtils;
    }

    @Override
    public long heartbeatInterval() {
        return this.config.getHeartbeatInterval();
    }

    @Override
    public int expirationCount() {
        return this.config.getExpirationCount();
    }

    @Override
    public long timestamp() {
        return System.currentTimeMillis();
    }

    /**
     * 获取节点过期时间阈值
     */
    @Override
    public long expirationTime() {
        return expirationCount() * heartbeatInterval();
    }

    /**
     * redis key格式化
     */
    private String redisFormat(String prefix, String suffix) {
        return String.format(REDIS_FORMAT, redisSplitEnd(prefix), redisSplitStart(suffix));
    }

    private String redisSplitEnd(String key) {
        return key.endsWith(REDIS_SPLIT) ? key : key.concat(REDIS_SPLIT);
    }

    private String redisSplitStart(String key) {
        return key.startsWith(REDIS_SPLIT) ? key.substring(FIRST) : key;
    }


    /**
     * 以下是对ITaskContext接口的实现
     */
    @Override
    public String leaderLock() {
        return redisFormat(prefix(), LEADER_LOCK);
    }

    @Override
    public String leaderName() {
        return redisFormat(prefix(), LEADER_NAME);
    }

    @Override
    public String getQueue(QueueType queueType) {
        return redisFormat(prefix(), queueType.getQueue());
    }

    @Override
    public List<String> getAllQueue() {
        return Arrays.asList(getQueue(QueueType.HIGN), getQueue(QueueType.MEDIUM), getQueue(QueueType.LOW));
    }

    @Override
    public List<QueueType> getAllQueueType() {
        return Arrays.asList(QueueType.HIGN, QueueType.MEDIUM, QueueType.LOW);
    }

    @Override
    public String resultQueue() {
        return redisFormat(prefix(), RESULT_QUEUE);
    }

    @Override
    public String masterConsumerLock() {
        return redisFormat(prefix(), MASTER_LOCK);
    }

    @Override
    public String slaveConsumerLock() {
        return redisFormat(prefix(), SLAVE_LOCK);
    }

    @Override
    public String executeHash() {
        return redisFormat(prefix(), EXECUTE_HASH);
    }

    @Override
    public String heartHash() {
        return redisFormat(prefix(), HEART_HASH);
    }

    @Override
    public String masterHashKey() {
        return redisFormat(MASTER_PREFIX, this.pidHostname);
    }

    @Override
    public String slaveHashKey() {
        return redisFormat(SLAVE_PREFIX, this.pidHostname);
    }

    @Override
    public List<String> getAllKey() {
        return Stream.of(getQueue(QueueType.HIGN), getQueue(QueueType.MEDIUM), getQueue(QueueType.LOW),
                resultQueue(), leaderLock(), leaderName(), masterConsumerLock(),
                slaveConsumerLock(), executeHash(), heartHash()).collect(Collectors.toList());
    }

    /**
     * 把新的value字符串添加到source字符串后面，中间用HASH_VALUE_SPLIT分隔
     * @param source
     * @param value
     * @return
     */
    @Override
    public String addSplit(Optional<String> source, String value) {
        Objects.requireNonNull(value);
        return source.map(s-> s.concat(HASH_VALUE_SPLIT).concat(value)).orElse(value);
    }
    // 拆分hash值
    @Override
    public List<String> deleteSplit(String value) {
        Objects.requireNonNull(value);
        return new ArrayList<>(Arrays.asList(value.split(HASH_VALUE_SPLIT_ESCAPE)));
    }
}
