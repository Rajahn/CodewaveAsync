package edu.vt.ranhuo.asynccore.service.rebalance.impl;

import edu.vt.ranhuo.asynccore.service.rebalance.Rebalance;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import org.redisson.api.RedissonClient;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.REBALANCE_MAP;

public class ConstantHashRebalanceImpl implements Rebalance {

    private final RedissonUtils redissonUtils;

    private TreeMap<Long, String> hashRing; // 哈希环

    public ConstantHashRebalanceImpl(RedissonClient redissonClient) {
        redissonUtils = RedissonUtils.getInstance(Optional.of(redissonClient));
        hashRing = new TreeMap<>();
    }

    @Override
    public void startRebalance(Set<String> activeNodes, int queueNum) {
        redissonUtils.del(REBALANCE_MAP);
        distributeQueuesAmongWorkers(activeNodes, queueNum);
    }

    @Override
    public List<Integer> getQueuesForWorker(String workerName, Set<String> activeNodes, int queueNum) {
        Optional<String> queuesString = redissonUtils.hget(REBALANCE_MAP, workerName);

        if (!queuesString.isPresent()) {
            // 如果没有找到对应的分配关系，触发重新分配
            distributeQueuesAmongWorkers(activeNodes, queueNum);
            queuesString = redissonUtils.hget(REBALANCE_MAP, workerName);
        }

        String queues = queuesString.orElse("[]");
        return Arrays.stream(queues.substring(1, queues.length() - 1).split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    @Override
    public void handleNodeFailure(String failedNodeName, Set<String> activeNodes, int queueNum) {
        handleNodeFailure(failedNodeName);
    }


    public void distributeQueuesAmongWorkers(Set<String> activeNodes,int queueNum) {

        final int VIRTUAL_NODES = 10000;

        // 将工作节点及其虚拟节点映射到哈希环上
        for (String node : activeNodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                // 为每个实际节点创建多个虚拟节点，并计算它们的哈希值
                String virtualNodeName = node + "#" + i; // 虚拟节点名称
                long hash = hashFunction(virtualNodeName);
                hashRing.put(hash, node); // 使用实际节点名称作为映射，以便最后分配队列
            }
        }

        Map<String, List<Integer>> nodeToQueuesMap = new HashMap<>(); // 节点到队列的映射

        // 为每个队列分配工作节点
        for (int i = 0; i < queueNum; i++) {
            long queueHash = hashFunction("queue" + i);
            String assignedNode = getAssignedWorkerNode(hashRing, queueHash);
            nodeToQueuesMap.computeIfAbsent(assignedNode, k -> new ArrayList<>()).add(i);
        }

        // 将节点到队列的映射关系存储到Redis
        for (Map.Entry<String, List<Integer>> entry : nodeToQueuesMap.entrySet()) {
            redissonUtils.hset(REBALANCE_MAP, entry.getKey(), entry.getValue().toString());
        }
    }

    private String getAssignedWorkerNode(TreeMap<Long, String> hashRing, long queueHash) {
        // 顺时针找到第一个节点
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(queueHash);
        if (entry == null) {
            // 如果没有找到，说明队列哈希值超过了环上的最大值，应该分配给环上的第一个节点
            entry = hashRing.firstEntry();
        }
        return entry.getValue();
    }

    private static long hashFunction(String key) {
        try {
            // 使用SHA-256哈希算法
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes());

            // 将前8字节转换为long值，以获得较大的散布空间
            long hash = new BigInteger(1, Arrays.copyOfRange(hashBytes, 0, 8)).longValue();

            // 确保哈希值为正数
            return hash & 0x7fffffffffffffffL;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 hash algorithm not found");
        }
    }

    public void handleNodeFailure(String failedNode) {
        // 从 Redis 中获取宕机节点负责的队列列表
        String failedNodeQueues = (String) redissonUtils.hget(REBALANCE_MAP, failedNode).orElse("[]");
        List<Integer> queuesToReassign = Arrays.stream(failedNodeQueues.substring(1, failedNodeQueues.length() - 1).split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        // 删除宕机节点的条目
        redissonUtils.hdel(REBALANCE_MAP, failedNode);
        //从hashring移除宕机节点及其虚拟节点
        final int VIRTUAL_NODES = 10000;
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            String virtualNodeName = failedNode + "#" + i;
            long hash = hashFunction(virtualNodeName);
            hashRing.remove(hash); // 从哈希环中删除虚拟节点
        }
        hashRing.remove(hashFunction(failedNode)); // 从哈希环中删除实际节点
        // 重新分配宕机节点的队列
        redistributeQueues(queuesToReassign);
    }

    private void redistributeQueues(List<Integer> queuesToReassign) {
        for (Integer queue : queuesToReassign) {
            long queueHash = hashFunction("queue" + queue);
            String assignedNode = getAssignedWorkerNode(hashRing, queueHash);

            // 获取当前节点已经负责的队列
            Optional<String> currentQueuesString = redissonUtils.hget(REBALANCE_MAP, assignedNode);
            List<Integer> currentQueues = new ArrayList<>();
            if (currentQueuesString.isPresent()) {
                currentQueues = Arrays.stream(currentQueuesString.get().substring(1, currentQueuesString.get().length() - 1).split(","))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
            }

            // 添加新分配的队列并更新节点到队列的映射关系
            if (!currentQueues.contains(queue)) {
                currentQueues.add(queue);
                redissonUtils.hset(REBALANCE_MAP, assignedNode, currentQueues.toString());
            }
        }
    }

}
