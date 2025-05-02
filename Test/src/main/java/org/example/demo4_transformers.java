package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class demo4_transformers {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        en.setParallelism(1);

       en.readTextFile("src/main/resources/socres").filter(new FilterFunction<String>() {
           @Override
           public boolean filter(String s) throws Exception {
               return !s.split(",")[0].equals("id");
           }
       }).flatMap(new FlatMapFunction<String, Tuple3<String,String,Integer>>() {

           @Override
           public void flatMap(String s, Collector<Tuple3<String, String,Integer>> collector) throws Exception {
               String[] arr  = s.split(",");
               String name  = arr[1];
               String[] scores = arr[2].split("\\|");
               String[] gemu=new String[]{"yuwen","yingyu","shuxue"};
               for (int i = 0; i < scores.length; i++) {
                   collector.collect(Tuple3.of(name,gemu[i],Integer.valueOf(scores[i])));
               }

           }
       });




        en.execute("aa");
    }
}
