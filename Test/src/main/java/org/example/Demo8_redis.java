package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Demo8_redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.BATCH);
        en.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> source = en.readTextFile("src/main/resources/socres").filter(x -> !x.split(",")[0].equals("id"))
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        Integer key = Integer.valueOf(s.split(",")[0]);
                        Integer sum = 0;
                        for (String one : s.split(",")[2].split("\\|")) {
                            sum = sum + Integer.valueOf(one);
                        }


                        return Tuple2.of(key, sum);
                    }
                });


        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.45.16")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();
     source.addSink(new RedisSink<>(redisConf, new RedisMapper<Tuple2<Integer, Integer>>() {
         @Override
         public RedisCommandDescription getCommandDescription() {
             return new RedisCommandDescription(RedisCommand.SET);
         }

         @Override
         public String getKeyFromData(Tuple2<Integer, Integer> integerIntegerTuple2) {
             return integerIntegerTuple2.f0.toString();
         }

         @Override
         public String getValueFromData(Tuple2<Integer, Integer> integerIntegerTuple2) {
             return integerIntegerTuple2.f1.toString();
         }
     }));

        en.execute("aa");
    }
}
