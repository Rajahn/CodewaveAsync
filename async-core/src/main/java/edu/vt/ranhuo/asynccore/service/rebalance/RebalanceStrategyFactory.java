package edu.vt.ranhuo.asynccore.service.rebalance;

import edu.vt.ranhuo.asynccore.service.rebalance.impl.ConstantHashRebalanceServiceImpl;
import edu.vt.ranhuo.asynccore.service.rebalance.impl.RoundRebalanceServiceImpl;
import edu.vt.ranhuo.asynccore.utils.RedissonUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import java.util.HashMap;
import java.util.Map;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.CONSISTENT_HASH;
import static edu.vt.ranhuo.asynccore.enums.CommonConstants.ROUND_ROBIN;

public class RebalanceStrategyFactory {
    private static final Map<String,RebalanceService> strategyMap = new HashMap<>();

    public static RebalanceService getRebalanceStrategy(String strategy, RedissonClient redissonClient){
        if(strategyMap.containsKey(strategy)){
            return strategyMap.get(strategy);
        }
        RebalanceService rebalanceService = null;
        switch (strategy){
            case CONSISTENT_HASH:
                rebalanceService = new ConstantHashRebalanceServiceImpl(redissonClient);
                break;
            case ROUND_ROBIN:
                rebalanceService = new RoundRebalanceServiceImpl(redissonClient);
                break;
            default:
                throw new IllegalArgumentException("Unknown strategy: " + strategy);
        }
        strategyMap.put(strategy,rebalanceService);
        return rebalanceService;
    }

}
