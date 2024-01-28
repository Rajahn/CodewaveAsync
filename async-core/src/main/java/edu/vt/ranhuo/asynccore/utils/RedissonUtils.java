package edu.vt.ranhuo.asynccore.utils;

import org.redisson.api.*;
import org.redisson.client.protocol.ScoredEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedissonUtils {
    private static final Logger log = LoggerFactory.getLogger(RedissonUtils.class);
    private static volatile RedissonUtils instance;
    private  RedissonClient redisson;

    private RedissonUtils(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public static RedissonUtils getInstance(Optional<RedissonClient> redisson) {
        if (instance == null) {
            Class var1 = RedissonUtils.class;
            synchronized (RedissonUtils.class) {
                if (instance == null) {
                    if (!redisson.isPresent()) {
                        throw new NullPointerException("build RedissonUtils failed, RedissonClient is null");
                    }

                    instance = new RedissonUtils((RedissonClient) redisson.get());
                }
            }
        }

        return instance;
    }

    public RedissonClient getRedisson() {
        return this.redisson;
    }

    public <T> Optional<T> get(String key) {
        RBucket<T> bucket = this.redisson.getBucket(key);
        return Optional.ofNullable(bucket.get());
    }

    public <T> void set(String key, T value) {
        RBucket<T> bucket = this.redisson.getBucket(key);
        bucket.set(value);
    }

    public <T> void setex(String key, T value, long timeToLive, TimeUnit timeUnit) {
        RBucket<T> bucket = this.redisson.getBucket(key);
        bucket.set(value, timeToLive, timeUnit);
    }

    public boolean del(String key) {
        return this.redisson.getBucket(key).delete();
    }

    public boolean exists(String key) {
        return this.redisson.getBucket(key).isExists();
    }

    public <T> Optional<List<T>> lrange(String key, int fromIndex, int toIndex) {
        RList<T> list = this.redisson.getList(key);
        return list.size() == 0 ? Optional.empty() : Optional.of(list.subList(fromIndex, toIndex));
    }

    public <T> Optional<T> lpop(String key) {
        RQueue<T> queue = this.redisson.getQueue(key);
        return Optional.ofNullable(queue.poll());
    }

    public <T> Optional<T> rpop(String key) {
        RDeque<T> deque = this.redisson.getDeque(key);
        return Optional.ofNullable(deque.pollLast());
    }

    public <T> Optional<T> rlpop(String key) {
        RList<T> list = this.redisson.getList(key);
        return Optional.ofNullable(list.get(0));
    }

    public <T> Optional<T> rrpop(String key) {
        RList<T> list = this.redisson.getList(key);
        return Optional.ofNullable(list.get(list.size() - 1));
    }

    public <T> Optional<T> rpoplpush(String source, String destination) {
        RDeque<T> deque = this.redisson.getDeque(source);
        return Optional.ofNullable(deque.pollLastAndOfferFirstTo(destination));
    }

    public void lrem(String key, int index) {
        if (this.exists(key)) {
            this.redisson.getList(key).fastRemove(index);
        }

    }

    public <V> void lpush(String key, V elements) {
        this.redisson.getDeque(key).addFirst(elements);
    }

    public <V> boolean rpush(String key, V elements) {
        return this.redisson.getList(key).add(elements);
    }

    public <V> void lset(String key, int index, V element) {
        if (this.exists(key)) {
            this.redisson.getList(key).fastSet(index, element);
        }

    }

    public int llen(String key) {
        return this.redisson.getList(key).size();
    }

    @SafeVarargs
    public final <K> long hdel(String key, K... keys) {
        return this.redisson.getMap(key).fastRemove(keys);
    }

    public <K> boolean hexists(String key, K k) {
        return this.redisson.getMap(key).containsKey(k);
    }

    public <K, V> Optional<V> hget(String key, K k) {
        RMap<K, V> map = this.redisson.getMap(key);
        return Optional.ofNullable(map.get(k));
    }

    public <K, V> Map<K, V> hgetall(String key) {
        RMap<K, V> map = this.redisson.getMap(key);
        return (Map) map.readAllEntrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SafeVarargs
    public final <K, V> Map<K, V> hmget(String key, K... k) {
        RMap<K, V> map = this.redisson.getMap(key);
        Set<K> collect = (Set) Arrays.stream(k).collect(Collectors.toSet());
        return map.getAll(collect);
    }

    public <K> Set<K> hkeys(String key) {
        RMap<K, Object> map = this.redisson.getMap(key);
        return map.readAllKeySet();
    }

    public <V> Collection<V> hvals(String key) {
        RMap<Object, V> map = this.redisson.getMap(key);
        return map.readAllValues();
    }

    public int hlen(String key) {
        return this.redisson.getMap(key).size();
    }

    public <K, V> void hset(String key, K field, V value) {
        RMap<K, V> map = this.redisson.getMap(key);
        map.put(field, value);
    }

    public <K, V> void hmset(String key, Map<? extends K, ? extends V> map, int batchSize) {
        this.redisson.getMap(key).putAll(map, batchSize);
    }

    public <V> boolean zadd(String key, double score, V v) {
        RScoredSortedSet<V> sortedSet = this.redisson.getScoredSortedSet(key);
        return sortedSet.add(score, v);
    }

    public int zcard(String key) {
        return this.redisson.getScoredSortedSet(key).size();
    }

    public <V> Collection<V> zrange(String key, int startIndex, int endIndex) {
        RScoredSortedSet<V> sortedSet = this.redisson.getScoredSortedSet(key);
        return sortedSet.valueRange(startIndex, endIndex);
    }

    public <V> Optional<V> zrpop(String key) {
        RScoredSortedSet<V> sortedSet = this.redisson.getScoredSortedSet(key);
        Iterator<V> iterator = sortedSet.valueRange(-1, -1).iterator();
        return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    }

    public <V> Collection<ScoredEntryEx<V>> zrangebyscore(String key, int startIndex, int endIndex) {
        RScoredSortedSet<V> sortedSet = this.redisson.getScoredSortedSet(key);
        return (Collection) sortedSet.entryRange(startIndex, endIndex).stream().map((x$0) -> {
            return new ScoredEntryEx(x$0);
        }).collect(Collectors.toList());
    }

    public <V> double zmax(String key) {
        RScoredSortedSet<V> sortedSet = this.redisson.getScoredSortedSet(key);
        Iterator<ScoredEntry<V>> iterator = sortedSet.entryRange(-1, -1).iterator();
        return iterator.hasNext() ? ((ScoredEntry) iterator.next()).getScore() : 0.0;
    }

    public <V> boolean zrem(String key, V v) {
        return this.redisson.getScoredSortedSet(key).remove(v);
    }

    public <V> Optional<V> eval(RScript.Mode mode, String lua, List<Object> keys, Object... values) {
        RScript script = this.redisson.getScript();
        V eval = script.eval(mode, lua, RScript.ReturnType.VALUE, keys, values);
        return Objects.nonNull(eval) ? Optional.of(eval) : Optional.empty();
    }

    public <T, R> Optional<R> lock(String lockKey, Optional<T> t, Function<Optional<T>, Optional<R>> func) {
        boolean isLock = false;
        Optional<R> result = Optional.empty();
        RLock lock = this.redisson.getLock(lockKey);
        String threadName = Thread.currentThread().getName();

        try {
            while (!isLock) {
                isLock = lock.tryLock(1L, TimeUnit.SECONDS);
                if (isLock) {
                    log.debug(String.format(" Lock successfully, execute the function, ThreadName: %s ", threadName));
                    result = (Optional) func.apply(t);
                } else {
                    log.debug(String.format(" Failed to lock. Try to lock, ThreadName: %s ", threadName));
                }
            }
        } catch (Throwable var12) {
            log.error(" Throwable locking ", var12);
        } finally {
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                log.debug(String.format(" Release lock , ThreadName: %s ", threadName));
                lock.unlock();
            }

        }

        return result;
    }

    public static final class ScoredEntryEx<V> {
        private final Double score;
        private final V value;

        private ScoredEntryEx(ScoredEntry<V> scoredEntry) {
            this.score = scoredEntry.getScore();
            this.value = scoredEntry.getValue();
        }

        public Double getScore() {
            return this.score;
        }

        public V getValue() {
            return this.value;
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            } else if (!(o instanceof ScoredEntryEx)) {
                return false;
            } else {
                ScoredEntryEx<?> other = (ScoredEntryEx) o;
                Object this$score = this.getScore();
                Object other$score = other.getScore();
                if (this$score == null) {
                    if (other$score != null) {
                        return false;
                    }
                } else if (!this$score.equals(other$score)) {
                    return false;
                }

                Object this$value = this.getValue();
                Object other$value = other.getValue();
                if (this$value == null) {
                    if (other$value != null) {
                        return false;
                    }
                } else if (!this$value.equals(other$value)) {
                    return false;
                }

                return true;
            }
        }

        public int hashCode() {
            boolean PRIME = true;
            int result = 1;
            Object $score = this.getScore();
            result = result * 59 + ($score == null ? 43 : $score.hashCode());
            Object $value = this.getValue();
            result = result * 59 + ($value == null ? 43 : $value.hashCode());
            return result;
        }

        public String toString() {
            return "RedissonUtils.ScoredEntryEx(score=" + this.getScore() + ", value=" + this.getValue() + ")";
        }
    }
}
