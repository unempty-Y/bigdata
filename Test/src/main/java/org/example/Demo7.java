package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo7 {
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
                         String [] title= new String[]{"语文","数学","英语"};
                          String[] scores=arr[2].split("\\|");
                          for (int i = 0; i < scores.length; i++) {
                              collector.collect(Tuple3.of(name,title[i],Integer.valueOf(scores[i])));

                          }


                      }
                  }).keyBy(x->x.f0).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                      @Override
                      public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> t1, Tuple3<String, String, Integer> t2) throws Exception {

                          return Tuple3.of(t1.f0,t1.f1+"|"+t2.f1,t1.f2+t2.f2);
                      }
                  })
                  .print();


        en.execute("aa");

    }
}
