package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo2_batch_stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        en.setParallelism(1);
        DataStreamSource<String> dataStreamSource = en.socketTextStream("192.168.31.128", 2000);
       dataStreamSource.map(new MapFunction<String, Tuple2<String ,Integer>>() {
           @Override
           public Tuple2<String, Integer> map(String s) throws Exception {
               String s1 = s.split(",")[1];


               return Tuple2.of(s1,1);
           }
       }).keyBy(x->x.f0).sum(1).print();


       en.execute("Demo2");

    }

}
