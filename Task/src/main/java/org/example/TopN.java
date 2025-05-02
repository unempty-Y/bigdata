package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Set;

public class TopN implements Constants, Serializable {
    public void addElem(String key, Double increment, String member) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        jedis.zincrby(key, increment, member);
    }
    public void addElem(String key, Integer increment, String member) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        jedis.zincrby(key, increment, member);
    }

    public Tuple2<String, Double>[] getTopN(String key, int start, int end, boolean enableAcs) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        Set<Tuple> data = null;
        if (enableAcs) {//降序排序
            data = jedis.zrevrangeWithScores(key, start, end);
        } else {//升序排序
            data = jedis.zrangeWithScores(key, start, end);
        }
        return data.stream()
                .map(i -> new Tuple2<>(i.getElement(), i.getScore()))
                .toArray(Tuple2[]::new);
    }

    public Tuple2<String, Double>[] getTopN(String key, int start, int end) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        Set<Tuple> data = null;
        if (true) {//降序排序
            data = jedis.zrevrangeWithScores(key, start, end);
        } else {//升序排序
            data = jedis.zrangeWithScores(key, start, end);
        }
        return data.stream()
                .map(i -> new Tuple2<>(i.getElement(), i.getScore()))
                .toArray(Tuple2[]::new);
    }

    public void cleanElem(String key) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        jedis.del(key);
    }

    public void cleanElem(String[] key) {
        Jedis jedis = new Jedis(bigdata1, redisPort);
        jedis.del(key);
    }
}
