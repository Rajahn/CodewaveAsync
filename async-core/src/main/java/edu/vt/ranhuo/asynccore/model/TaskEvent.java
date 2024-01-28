package edu.vt.ranhuo.asynccore.model;

import edu.vt.ranhuo.asynccore.enums.OverTime;
import edu.vt.ranhuo.asynccore.enums.QueueType;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.DOUBLE_FIRST;
import static edu.vt.ranhuo.asynccore.enums.CommonConstants.EXPIRATION_COUNT;
@Data
@Builder
public class TaskEvent<E> implements Event<E> {
    @NonNull
    private E value;
    @Builder.Default
    private double score = DOUBLE_FIRST;
    @Builder.Default
    private QueueType queue = QueueType.MEDIUM;
    @Builder.Default
    private long creatTime = EXPIRATION_COUNT;
    // 单位秒
    // 放入待执行队列时间
    private long queueTime;
    // 放入执行队列时间
    private long execTime;
    // 放入结果队列时间
    private long resQueueTime;
    // 放入结果队列执行时间
    private long resExecTime;
    // 执行队列超时时间
    @Builder.Default
    private long execOverTime = OverTime.SHOURS.getOverTime();
    // 结果执行队列超时时间
    @Builder.Default
    private long resExecOverTime = OverTime.OHOURS.getOverTime();
}
