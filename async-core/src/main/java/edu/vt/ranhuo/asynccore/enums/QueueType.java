package edu.vt.ranhuo.asynccore.enums;

import lombok.Getter;

@Getter
public enum QueueType {
    HIGN(CommonConstants.HIGN_QUEUE),
    MEDIUM(CommonConstants.MEDIUM_QUEUE),
    LOW(CommonConstants.LOW_QUEUE);

    private final String queue;

    QueueType(String queue) {
        this.queue = queue;
    }
}
