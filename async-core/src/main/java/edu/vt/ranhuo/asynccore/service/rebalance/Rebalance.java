package edu.vt.ranhuo.asynccore.service.rebalance;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public interface Rebalance {
    void startRebalance(Set<String> activeNodes, int queueNum);
    List<Integer> getQueuesForWorker(String workerName, Set<String> activeNodes, int queueNum);
    void handleNodeFailure(String failedNodeName, Set<String> activeNodes, int queueNum);
}
