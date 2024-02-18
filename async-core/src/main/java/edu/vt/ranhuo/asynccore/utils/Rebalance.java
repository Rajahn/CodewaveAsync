package edu.vt.ranhuo.asynccore.utils;

public interface Rebalance {
    RedissonUtils redissonUtils = null;

    public void startRebalance();
}
