package edu.vt.ranhuo.asynccore.service.rebalance.impl;
import edu.vt.ranhuo.asynccore.service.rebalance.RebalanceService;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import org.redisson.api.RedissonClient;
import java.util.*;
import java.util.stream.Collectors;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.REBALANCE_MAP;

public class RoundRebalanceServiceImpl implements RebalanceService {

    private final RedissonUtils redissonUtils;

    public RoundRebalanceServiceImpl(RedissonClient redissonClient) {
        redissonUtils = RedissonUtils.getInstance(Optional.of(redissonClient));
    }

    public void startRebalance(Set<String> activeNodes,int queueNum) {
        // 获取现有的key并清空对应的值, 避免触发多次缓存重建
        Map<String, String> existingKeys = redissonUtils.hgetall(REBALANCE_MAP);
        for (String key : existingKeys.keySet()) {
            redissonUtils.hset(REBALANCE_MAP, key, "[]");
        }

        List<String> nodes = new ArrayList<>(activeNodes);
        int nodeNum = nodes.size();
        int minQueuesPerNode = queueNum / nodeNum;
        int extraQueues = queueNum % nodeNum;
        Map<String, List<Integer>> nodeQueueMap = new HashMap<>();


        if(nodeNum>queueNum){
            for (int i = 0; i < nodeNum; i++) {
                List<Integer> queuesForNode = new ArrayList<>();
                queuesForNode.add(i % queueNum);
                nodeQueueMap.put(nodes.get(i), queuesForNode);
            }
            for (Map.Entry<String, List<Integer>> entry : nodeQueueMap.entrySet()) {
                // 保存节点与队列的映射关系到Redis
                redissonUtils.hset(REBALANCE_MAP, entry.getKey(), entry.getValue().toString());
            }
            return;
        }

        int queueIndex = 0;
        for (String node : nodes) {
            List<Integer> queuesForNode = new ArrayList<>();
            for (int i = 0; i < minQueuesPerNode; i++) {
                queuesForNode.add(queueIndex % queueNum);
                queueIndex++;
            }
            if (extraQueues > 0) {
                queuesForNode.add(queueIndex % queueNum);
                queueIndex++;
                extraQueues--;
            }
            nodeQueueMap.put(node, queuesForNode);
        }

        for (Map.Entry<String, List<Integer>> entry : nodeQueueMap.entrySet()) {
            redissonUtils.hset(REBALANCE_MAP, entry.getKey(), entry.getValue().toString());
        }
    }

    @Override
    public void handleNodeFailure(String failedNodeName, Set<String> activeNodes, int queueNum) {
        startRebalance(activeNodes,queueNum);
        redissonUtils.hdel(REBALANCE_MAP,failedNodeName);
    }

    public List<Integer> getQueuesForWorker(String workerName,Set<String> activeNodes,int queueNum) {
        Optional<String> queuesString = redissonUtils.hget(REBALANCE_MAP, workerName);

        if (!queuesString.isPresent()) {
            // 没有找到对应的分配关系，需要重新初始化
            startRebalance(activeNodes,queueNum);
            queuesString = redissonUtils.hget(REBALANCE_MAP, workerName);
        }
        // 解析队列编号并返回
        String queues = queuesString.orElse("[]");

        return Arrays.stream(queues.substring(1, queues.length() - 1).split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty()) // 过滤掉空字符串
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }
}

