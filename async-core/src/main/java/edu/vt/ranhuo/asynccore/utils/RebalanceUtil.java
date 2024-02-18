package edu.vt.ranhuo.asynccore.utils;
import org.redisson.api.RedissonClient;
import java.util.*;
import java.util.function.Supplier;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.REBALANCE_MAP;

public class RebalanceUtil {

    private RedissonUtils redissonUtils;

    public RebalanceUtil(RedissonClient redissonClient) {
        this.redissonUtils = RedissonUtils.getInstance(Optional.of(redissonClient));
    }

    public void startReBalance(Supplier<Set<String>> activeNodesSupplier,int queueNum) {
        Set<String> activeNodes = activeNodesSupplier.get();
        //int queueNum = 25; // 总队列数量，需要根据实际情况获取

        redissonUtils.del(REBALANCE_MAP);

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

}

