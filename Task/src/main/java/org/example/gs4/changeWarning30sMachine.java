package org.example.gs4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.*;

import java.text.SimpleDateFormat;

public class changeWarning30sMachine implements Constants {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        String topic = "ChangeRecord";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<Tuple3<String, String, String>> warnings = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .readTextFile("E:\\input\\2024-06-07@22_08-changerecord.txt")
                .filter(s -> s.split(",")[3].equals("预警"))
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple3(split[1], split[4], split[5]);
                    }
                });
        ProcessFunction<Tuple3<String, String, String>, Tuple2<String, String>> processFunction = new ProcessFunction<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public void processElement(Tuple3<String, String, String> stringStringStringTuple3, ProcessFunction<Tuple3<String, String, String>, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long timeOn = simpleDateFormat.parse(stringStringStringTuple3.f1).getTime();
                long timeEnd = simpleDateFormat.parse(stringStringStringTuple3.f2).getTime();
                long i = (timeEnd - timeOn) / 30000;
                while (i > 0) {
                    timeOn = timeOn + 30000L;
                    String warningTime = simpleDateFormat.format(timeOn);
                    String s = warningTime+":设备" + stringStringStringTuple3.f0 + "连续30秒为预警状态请尽快处理！";
                    collector.collect(new Tuple2<>(stringStringStringTuple3.f0, s));
                    i--;
                }
            }
        };
        SingleOutputStreamOperator<Tuple2<String, String>> process = warnings.process(processFunction);
        process.print();
//        process.addSink(new RedisWriter().redisHSet("warning30sMachine"));
        env.execute("changeWarning30sMachine");
    }
}
