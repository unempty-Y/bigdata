package org.example.gs5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.*;

import java.text.SimpleDateFormat;
import java.util.Date;

public class produceCount5minuteHbase implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ProduceRecord";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<ProduceRecord> produceRecords = env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, ProduceRecord>() {
                    @Override
                    public ProduceRecord map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new ProduceRecord(Integer.valueOf(split[0]), Integer.valueOf(split[1]), split[2], split[3], split[4], split[5],
                                Integer.valueOf(split[6]), split[7], Integer.valueOf(split[8]), Integer.valueOf(split[9]));
                    }
                })
                .assignTimestampsAndWatermarks(new Watermarks().produceRecordWatermarkStrategy());
        SingleOutputStreamOperator<ProduceRecord> checkOn = produceRecords
                .filter(produceRecord -> produceRecord.getProduceInspect() == 1);
        SingleOutputStreamOperator<Tuple2<String, Tuple3<String, String, String>[]>> sum = checkOn
                .map(new MapFunction<ProduceRecord, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(ProduceRecord produceRecord) throws Exception {
                        return new Tuple2<String, Integer>(String.valueOf(produceRecord.getProduceMachineID()), produceRecord.getProduceTotalOut());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Tuple3<String, String, String>[]>>() {
                    @Override
                    public Tuple2<String, Tuple3<String, String, String>[]> map(Tuple2<String, Integer> produce) throws Exception {
                        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date());
                        String rowkey = produce.f0 + "-" + date;
                        Tuple3<String, String, String>[] tuple3s = new Tuple3[2];
                        tuple3s[0]=new Tuple3<>("info","machine_id",produce.f0);
                        tuple3s[1]=new Tuple3<>("info","total_produce",produce.f1+"");
                        return new Tuple2<>(rowkey, tuple3s);
                    }
                });
        sum.print();
//        sum.addSink(new HbaseWriter().hbaseWriter("gyflinkresult:Produce5minAgg"));
        env.execute("produceCount5minuteHbase");
    }

}
