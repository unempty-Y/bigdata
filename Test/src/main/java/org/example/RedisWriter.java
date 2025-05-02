package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class RedisWriter implements Constants, Serializable {
    private Jedis jedis;

    public RichSinkFunction<String> redisSet(String key) {
        return new RichSinkFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis(bigdata1, redisPort);
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                jedis.set(key, value);
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        };
    }

    public RichSinkFunction<Tuple2<String, String>> redisHSet(String key) {
        return new RichSinkFunction<Tuple2<String, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis(bigdata1, redisPort);
            }

            @Override
            public void invoke(Tuple2<String, String> value, Context context) throws Exception {
                jedis.hset(key, value.f0, value.f1);
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        };
    }
}