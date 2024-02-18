package edu.vt.ranhuo.asynccore.service.rebalance.impl;

import edu.vt.ranhuo.asynccore.service.leader.impl.LeaderServiceImplTest;
import edu.vt.ranhuo.asynccore.service.rebalance.Rebalance;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class RoundRebalanceImplTest {

    private Rebalance rebalance;

    Set<String> activeNodes = new HashSet<>(Arrays.asList("node1", "node2", "node3"));
    @Before
    public void setUp() throws Exception {
        final URL resource = LeaderServiceImplTest.class.getClassLoader().getResource("redisson.yml");
        RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        rebalance = new RoundRebalanceImpl(redissonClient);
    }

    @Test
    public void startRebalance() {
        rebalance.startRebalance(activeNodes,2);
    }

    @Test
    public void handleNodeFailure() {
        rebalance.handleNodeFailure("node1",new HashSet<>(Arrays.asList("node2","node3")),2);
    }

    @Test
    public void getQueuesForWorker() {
        List<Integer> queues = rebalance.getQueuesForWorker("node1",activeNodes,2);
        System.out.println(queues);
    }
}