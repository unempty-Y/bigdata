package org.example.gs5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.ChangeRecord;
import org.example.KafkaConnect;
import org.example.RedisWriter;

import java.text.SimpleDateFormat;
import java.util.Date;

public class warningLast3minCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "ChangeRecord";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<ChangeRecord> changeRecordSingleOutputStreamOperator = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, ChangeRecord>() {
                    @Override
                    public ChangeRecord map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new ChangeRecord(
                                split[0],
                                Integer.parseInt(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                split[4],
                                split[5],
                                Integer.parseInt(split[6])
                        );
                    }
                });
        changeRecordSingleOutputStreamOperator.print();

        SingleOutputStreamOperator<String> warning1for3min = changeRecordSingleOutputStreamOperator
                .filter(changeRecord -> changeRecord.getChangeRecordState().equals("预警"))
                .map(new MapFunction<ChangeRecord, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(ChangeRecord changeRecord) throws Exception {
                        return new Tuple2<String, Double>(changeRecord.getChangeMachineRecordID() + "", 1.0);
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.minutes(1)))
                .sum(1)
                .keyBy(i->true)
                .max(1)
                .map(new MapFunction<Tuple2<String, Double>, String>() {
                    @Override
                    public String map(Tuple2<String, Double> tuple2) throws Exception {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String date = dateFormat.format(new Date());
                        return date + "," + tuple2.f0;
                    }
                });
        warning1for3min.print();
        warning1for3min.addSink(new RedisWriter().redisSet("warning_last3min_everymin_out"));
        env.execute("warningLast3minCount");
    }
}
