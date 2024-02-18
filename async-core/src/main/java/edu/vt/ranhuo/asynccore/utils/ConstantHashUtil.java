package edu.vt.ranhuo.asynccore.utils;

import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class ConstantHashUtil {
    private static RedissonUtils redissonUtils;

    private static final String NODE_QUEUE_MAP_KEY = "constanthash:nodeQueueMap"; // Redis key for node-to-queue map

    private TreeMap<Long, String> hashRing; // 哈希环

    public ConstantHashUtil(RedissonClient redissonClient) {
        this.redissonUtils = RedissonUtils.getInstance(Optional.of(redissonClient));
    }

    public void initializeQueuesInHashRing(int queueNum) {
        String hashRingKey = "constanthash:hashRingQueues"; // Redis key for the hash ring

        // Clear existing hash ring data
        redissonUtils.del(hashRingKey);

        for (int i = 0; i < queueNum; i++) {
            String queueId = "queue" + i;
            long hash = hashFunction(queueId);
            // Add queue ID with its hash value to the Redis sorted set
            redissonUtils.zadd(hashRingKey, hash, queueId);
        }
        hashRing = new TreeMap<>();
        distributeQueuesAmongWorkers(queueNum);
    }

    public void handleNodeFailure(String failedNode) {
        // 从 Redis 中获取宕机节点负责的队列列表
        String failedNodeQueues = (String) redissonUtils.hget(NODE_QUEUE_MAP_KEY, failedNode).orElse("[]");
        List<Integer> queuesToReassign = Arrays.stream(failedNodeQueues.substring(1, failedNodeQueues.length() - 1).split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        // 删除宕机节点的条目
        redissonUtils.hdel(NODE_QUEUE_MAP_KEY, failedNode);
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
            Optional<String> currentQueuesString = redissonUtils.hget(NODE_QUEUE_MAP_KEY, assignedNode);
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
                redissonUtils.hset(NODE_QUEUE_MAP_KEY, assignedNode, currentQueues.toString());
            }
        }
    }

    private TreeMap<Long, String> buildHashRing(Set<String> activeNodes) {
        TreeMap<Long, String> hashRing = new TreeMap<>();
        final int VIRTUAL_NODES = 10000;

        for (String node : activeNodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNodeName = node + "#" + i;
                long hash = hashFunction(virtualNodeName);
                hashRing.put(hash, node);
            }
        }
        return hashRing;
    }

    public void distributeQueuesAmongWorkers(int queueNum) {
        Set<String> activeNodes = getActiveNodeInfo();

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
            redissonUtils.hset(NODE_QUEUE_MAP_KEY, entry.getKey(), entry.getValue().toString());
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

    private Set<String> getActiveNodeInfo() {
        return new HashSet<>(Arrays.asList("worker2","worker1","worker3")); // 示例代码，需要根据实际情况获取
    }


    // 工作节点查询自己负责的队列列表
    public List<Integer> getQueuesForWorker(String workerName) {
        Optional<String> queuesString = redissonUtils.hget(NODE_QUEUE_MAP_KEY, workerName);

        if (!queuesString.isPresent()) {
            // 如果没有找到对应的分配关系，触发重新分配
            distributeQueuesAmongWorkers(3); // 假设有10个队列，需要根据实际情况调整
            queuesString = redissonUtils.hget(NODE_QUEUE_MAP_KEY, workerName);
        }

        String queues = queuesString.orElse("[]");
        return Arrays.stream(queues.substring(1, queues.length() - 1).split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
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

    public static void main(String[] args) throws IOException, InterruptedException {
        final URL resource = ConstantHashUtil.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));

        ConstantHashUtil constantHashUtil = new ConstantHashUtil(redissonClient);
        constantHashUtil.initializeQueuesInHashRing(10); // Initialize for 10 queues

        constantHashUtil.handleNodeFailure("worker1");
        System.out.println(constantHashUtil.getQueuesForWorker("worker2"));
        Thread.sleep(1000); // Wait for the hash ring to be initialized

        //System.out.println(constantHashUtil.getQueuesForWorker("worker2"));

//        List<Integer> queues = ConstantHashUtil.getQueuesForWorker("worker1");
//        System.out.println("Queues assigned to worker1: " + queues);
//        List<Integer> queues1 = ConstantHashUtil.getQueuesForWorker("worker1");
//        System.out.println("Queues assigned to worker2: " + queues);
//        List<Integer> queues2 = ConstantHashUtil.getQueuesForWorker("worker1");
//        System.out.println("Queues assigned to worker3: " + queues);
    }
}
