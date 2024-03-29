package edu.vt.ranhuo.asynccore.service.leader.impl;

import edu.vt.ranhuo.asynccore.config.TaskContext;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import edu.vt.ranhuo.asynccore.exceptions.HashPrefixException;
import edu.vt.ranhuo.asynccore.lambda.ProcessLambda;
import edu.vt.ranhuo.asynccore.service.leader.LeaderService;
import edu.vt.ranhuo.asynccore.service.rebalance.RebalanceService;
import edu.vt.ranhuo.asynccore.service.rebalance.RebalanceStrategyFactory;
import edu.vt.ranhuo.asynccore.service.rebalance.impl.RoundRebalanceServiceImpl;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.*;

@Slf4j
public class LeaderServiceImpl implements LeaderService {

    private final AtomicBoolean stopper = new AtomicBoolean();
    private final TaskContext context;
    private final RedissonUtils redissonUtils;
    private final Thread heartThread;
    private final Thread leaderThread;
    private final String nodeInfo;
    private RebalanceService rebalancer;

    public LeaderServiceImpl(TaskContext context, String nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.context = context;
        this.heartThread = new Thread(() -> process(this::heart));
        this.leaderThread = new Thread(this::seize);
        this.redissonUtils = RedissonUtils.getInstance(Optional.empty());
        this.rebalancer = RebalanceStrategyFactory.getRebalanceStrategy(context.getConfig().getRebalanceStrategy(), redissonUtils.getRedisson());
        init();
    }

    @Override
    public void init() {
        this.heartThread.start();
        this.leaderThread.start();
    }

    @Override
    public void heart() {
        final long timestamp = context.timestamp();
        try {
            redissonUtils.hset(context.heartHash(), nodeInfo, timestamp);
            log.info("send heart, hashkey: {}, nodeInfo: {}, timestamp: {}", context.heartHash(), nodeInfo, timestamp);
        } catch (Exception e) {
            log.error("Failed to send heart due to Redis exception: {}", e.getMessage(), e);
        }
    }


    @Override
    public void seize() {
        redissonUtils.lock(context.leaderLock(), Optional.empty(), (t) -> {
            listen();
            return t;
        });
    }

    @Override
    public void listen() {
        try {
            redissonUtils.set(context.leaderName(), nodeInfo);
            log.info("leader login was successful, key: {}, value: {}", context.leaderName(), nodeInfo);
            process(this::listener);
        } catch (Exception e) {
            log.error("Failed to listen due to Redis exception: {}", e.getMessage(), e);
        }
    }


    @Override
    public List<Integer> getQueuesForWorker(String workerName) {
        return rebalancer.getQueuesForWorker(workerName,getActiveSlaveNode(),context.getQueueNums());
    }


    @Override
    public Set<String> getActiveNodeInfo() {
        Map<String, Long> heartMap = redissonUtils.hgetall(context.heartHash());
        return heartMap.keySet();
    }

    private Set<String> getActiveSlaveNode() {
        Map<String, Long> heartMap = redissonUtils.hgetall(context.heartHash());
        return heartMap.keySet().stream().filter(k -> k.startsWith(SLAVE_PREFIX)).collect(Collectors.toSet());
    }

    @Override
    public Optional<String> getLeaderInfo() {
        return redissonUtils.get(context.leaderName());
    }

    private void listener() {
        Map<String, Long> heartMap = redissonUtils.hgetall(context.heartHash());
        long timestamp = context.timestamp();
        log.info("listen heart, leader: {}, redisKey:{}, heartMap: {}, timestamp: {}", redissonUtils.get(context.leaderName()),
                context.heartHash(), heartMap, timestamp);
        heartMap.forEach((k, v) -> {
            if (timestamp - v > context.expirationTime()) {
                Optional<String> value = redissonUtils.hget(context.executeHash(), k);
                log.warn("Node downtime processing start, k: {}, v: {}, timestamp: {}, executeHash: {}, executeValue: {}", k, v,
                        timestamp, context.executeHash(), value);
                if (k.startsWith(MASTER_PREFIX)) {
                    value.ifPresent(this::acceptMaster);
                } else if (k.startsWith(SLAVE_PREFIX)) {
                    value.ifPresent(this::acceptSlave);
                } else {
                    throw new HashPrefixException(String.format("executeHash key prefix is not present, key: %s", k));
                }
                redissonUtils.hdel(context.executeHash(), k);
                // 假设redis宕机后重启，这期间所有的executor的heart都过期了，leader的监听就会开始工作然后将heartHash删除
                // 不过没关系，executor还会间隔一段时间后重新注册
                redissonUtils.hdel(context.heartHash(), k);
                rebalancer.handleNodeFailure(k,getActiveSlaveNode(),context.getQueueNums()); // 重新分配任务队列与工作节点的对应关系
                log.warn("node downtime processing Successful, delete old executeHash: {}, delete old heartKey: {}, ", k, k);
            }
        });
    }

    /**
     * 若master节点宕机, 则将执行中数据存储至结果队列头部
     */
    public void acceptMaster(String value) {
        context.deleteSplit(value).forEach((v) -> redissonUtils.lpush(context.resultQueue(), v));
        log.warn("master node downtime processing end, value: {}, to resultQueue: {}", value, context.resultQueue());
    }

    /**
     *  若slave节点宕机, 则将执行中数据存储至高优队列最高优先级
     */
    public void acceptSlave(String value) {
        double zmax = redissonUtils.zmax(context.getQueue(QueueType.ONE));
        context.deleteSplit(value)
                .forEach((v) -> redissonUtils.zadd(context.getQueue(QueueType.ONE), zmax, v));
        log.warn("slave node downtime processing end, value: {}, score: {} to hignQueue: {}", value, zmax, context.getQueue(QueueType.ONE));
    }


    public void process(ProcessLambda lambda) {
        while (!stopper.get()) {
            lambda.process();
            waiting(context.heartbeatInterval());
        }
    }

    private void waiting(long interval) {
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            log.error("thread sleep error!", e);
        }
    }

    @Override
    public void close() {
        this.stopper.set(TRUE);
        log.warn("shutdown nodeInfo: {} ", nodeInfo);
    }
}
