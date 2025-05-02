package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class demo1_batch_stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.BATCH);
        en.setParallelism(1);
        en.readTextFile("src/main/resources/students.txt")
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String name  = s.split(",")[1];
                        return Tuple2.of(name,1);
                    }
                })
                .keyBy(0).sum(1)
                .print();


        en.execute("demo1_batch_stream");

    }
}
