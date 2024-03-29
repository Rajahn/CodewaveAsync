package edu.vt.ranhuo.asynccore.utils;

import edu.vt.ranhuo.asynccore.enums.QueueType;

import java.util.Random;

/**
 * 队列选择器
 * 1 以循环方式选择队列
 * 2 以随机方式选择队列，根据score的不同，会有不同的权重
 */
public class QueueSelector {

    private final int num;
    private final Random random;

    private int currentQueue;

    public QueueSelector(int num) {
        if (num < 1 || num > 9) {
            throw new IllegalArgumentException("num must be between 1 and 9.");
        }
        this.num = num;
        this.currentQueue = 0; // 初始化为0，下一次调用时返回1号队列
        this.random = new Random();
    }

    public static QueueType mapIntToQueueType(int queueNumber) {
        if (queueNumber < 1 || queueNumber > 9) {
            throw new IllegalArgumentException("Queue number must be between 1 and 9.");
        }
        return QueueType.values()[queueNumber - 1];
    }

    public int getQueueForMaster(double timestamp) {
        long currentTime = System.currentTimeMillis();
        long taskTime = (long) (timestamp * 1000); // 假设传入的timestamp是秒为单位的UNIX时间戳
        long diff = currentTime - taskTime;

        if (diff < -30000) { // 提前30秒以上，高优先级任务
            return weightedRandomQueue(new int[]{50, 30, 20, 10, 5, 3, 2, 1, 1});
        } else if (diff < -10000) { // 提前10秒以上，中优先级任务
            return weightedRandomQueue(new int[]{30, 25, 20, 15, 10, 5, 3, 2, 1});
        } else { // 其他情况，普通优先级任务
            return weightedRandomQueue(new int[]{20, 18, 15, 12, 10, 8, 6, 4, 2});
        }
    }

    public int getQueueForSlave() {
        // 为slave优先返回序号小的队列
        return 1 + random.nextInt(num);
    }

    public int getNextQueue() {
        currentQueue = (currentQueue % num) + 1; // 更新当前队列序号
        return currentQueue;
    }

    private int weightedRandomQueue(int[] weights) {
        // 计算总权重
        int totalWeight = 0;
        for (int i = 0; i < num && i < weights.length; i++) {
            totalWeight += weights[i];
        }

        // 生成一个随机数决定选哪个队列
        int randomIndex = random.nextInt(totalWeight);
        int sum = 0;
        for (int i = 0; i < num; i++) {
            sum += weights[i];
            if (randomIndex < sum) {
                return i + 1; // 队列序号从1开始
            }
        }

        return num; // 默认返回最后一个队列
    }
}
