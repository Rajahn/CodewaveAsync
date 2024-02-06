package edu.vt.ranhuo.asynccore.config;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.redisson.api.RedissonClient;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Data
@Builder
public class TaskConfig implements ITaskConfig{
    @Builder.Default
    private String prefix = PREFIX;
    @NonNull
    private RedissonClient redissonClient;
    @Builder.Default
    private long heartbeatInterval = HEARTBEAT_INTERVAL;
    @Builder.Default
    private int expirationCount = EXPIRATION_COUNT;

    @Builder.Default
    private int queueNums = QUEUE_NUMS;
    @Override
    public int getExpirationCount() { //不能小于3
        return Math.max(expirationCount, MIN_EXPIRATION_COUNT);
    }
    @Override
    public long getHeartbeatInterval() { //不能小于10s
        return Math.max(heartbeatInterval, MIN_HEARTBEAT_INTERVAL);
    }
}
