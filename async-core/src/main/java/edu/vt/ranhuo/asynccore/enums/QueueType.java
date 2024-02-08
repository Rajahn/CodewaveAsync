package edu.vt.ranhuo.asynccore.enums;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public enum QueueType {
    ONE(CommonConstants.QUEUE_ONE),
    TWO(CommonConstants.QUEUE_TWO),
    THREE(CommonConstants.QUEUE_THREE),
    FOUR(CommonConstants.QUEUE_FOUR),
    FIVE(CommonConstants.QUEUE_FIVE),
    SIX(CommonConstants.QUEUE_SIX),
    SEVEN(CommonConstants.QUEUE_SEVEN),
    EIGHT(CommonConstants.QUEUE_EIGHT),
    NINE(CommonConstants.QUEUE_NINE);

    private final String queue;

    QueueType(String queue) {
        this.queue = queue;
    }


    // 根据输入的数字返回枚举对象列表
    public static List<QueueType> getEnumsUpTo(int num) {
        List<QueueType> result = new ArrayList<>();
        if (num <= 0 || num > values().length) {
            throw new IllegalArgumentException("Invalid number: " + num);
        }
        for (int i = 0; i < num; i++) {
            result.add(values()[i]);
        }
        return result;
    }
}
