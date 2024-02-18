package edu.vt.ranhuo.asynccore.enums;

public enum CommonConstants {
    ;
    public static final String PREFIX = "codewave:default";
    public static final String MASTER_PREFIX = "master";
    public static final String SLAVE_PREFIX = "slave";

    public static final String MASTER_LOCK = "lock:consume:master";
    public static final String SLAVE_LOCK = "lock:consume:slave:";

    public static final String QUEUE_ONE = "queue:1";

    public static final String QUEUE_TWO = "queue:2";

    public static final String QUEUE_THREE = "queue:3";

    public static final String QUEUE_FOUR = "queue:4";

    public static final String QUEUE_FIVE = "queue:5";

    public static final String QUEUE_SIX = "queue:6";

    public static final String QUEUE_SEVEN = "queue:7";

    public static final String QUEUE_EIGHT = "queue:8";

    public static final String QUEUE_NINE = "queue:9";

    public static final String RESULT_QUEUE = "queue:result";

    public static final String EXECUTE_HASH = "hash:execute";
    public static final String HEART_HASH = "hash:heart";
    public static final String HASH_VALUE_SPLIT = "^codewave^";
    public static final String HASH_VALUE_SPLIT_ESCAPE = "\\^codewave\\^";

    public static final String LEADER_LOCK = "leader:lock";
    public static final String LEADER_NAME = "leader:name";

    public static final String REBALANCE_MAP = "rebalance:nodeQueueMap"; // Redis中保存节点分配关系的hash key

    public static String HASHRING_KEY = "rebalance:hashRingQueues"; // Redis key for the hash ring

    public static final String REDIS_FORMAT = "%s%s";
    public static final String EMPTY_STRING = "";
    public static final String REDIS_SPLIT = ":";

    public static final long MILLISECOND = 1000;
    public static final long HEARTBEAT_INTERVAL = 30 * MILLISECOND;
    public static final long MIN_HEARTBEAT_INTERVAL = 10 * MILLISECOND;
    public static final int EXPIRATION_COUNT = 6;
    public static final int MIN_EXPIRATION_COUNT = 3;
    public static final int FIRST = 1;
    public static final int SECOND = 2;
    public static final int THIRD = 3;
    public static final int FOURTH = 4;
    public static final int FIFTH = 5;
    public static final int SIXTH = 6;
    public static final int SEVENTH = 7;
    public static final int EIGHT = 8;
    public static final int TENTH = 10;
    public static final int ZERO = 0;
    public static final int END_INDEX = -1;

    public static final boolean FALSE = false;
    public static final boolean TRUE = true;
    public static final int QUEUE_NUMS = 1;
}
