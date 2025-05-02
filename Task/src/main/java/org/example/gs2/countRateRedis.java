package org.example.gs2;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.*;

import java.util.Arrays;


public class countRateRedis implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ods_mall_data";

        OutputTag<Integer> refund = new OutputTag<Integer>("refund") {
        };
        OutputTag<OrderMaster> cancel = new OutputTag<OrderMaster>("cancel") {
        };
        WatermarkStrategy<OrderMaster> orderMasterWatermarkStrategy = new Watermarks().orderMasterWatermarkStrategy();
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        ProcessFunction<OrderMaster, String> processFunction = new ProcessFunction<OrderMaster, String>() {
            transient ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("totalcountState", Integer.class));
            }

            @Override
            public void processElement(OrderMaster orderMaster, ProcessFunction<OrderMaster, String>.Context context, Collector<String> collector) throws Exception {
                if (Arrays.asList(filter).contains(orderMaster.getOrder_status())) {
                    Integer value = state.value();
                    if (value == null) value = 0;
                    int i = value + 1;
                    state.update(i);
                    collector.collect(i + "");
                } else if (orderMaster.getOrder_status().equals("已下单")) context.output(refund, 1);
                else if (orderMaster.getOrder_status().equals("已退款")) context.output(cancel, orderMaster);
            }
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OdsMallData> odsMallDataStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(i -> {
                    Gson gson = new GsonBuilder().create();
                    OdsMallData odsMallData = gson.fromJson(i, OdsMallData.class);
                    return odsMallData;
                });

        SingleOutputStreamOperator<OrderMaster> order_master = odsMallDataStream.filter(i -> i.getTable().equals("order_master"))
                .map(i -> {
                    Gson gson = new GsonBuilder().create();
                    String json = gson.toJson(i.getData());
                    OrderMaster orderMaster = gson.fromJson(json, OrderMaster.class);
                    return orderMaster;
                }).assignTimestampsAndWatermarks(orderMasterWatermarkStrategy);

        SingleOutputStreamOperator<String> dwdData = order_master
                .keyBy(i -> true)
                .process(processFunction);

        DataStream<String> refundSum = dwdData
                .getSideOutput(refund)
                .keyBy(i -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(0).map(integer -> integer.toString());

        SingleOutputStreamOperator<Tuple2<String, Integer>> orderCounts = order_master
                .map(new MapFunction<OrderMaster, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(OrderMaster orderMaster) {
                        return new Tuple2<>("orderCount", 1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> cancelCounts = dwdData
                .getSideOutput(cancel)
                .map(new MapFunction<OrderMaster, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(OrderMaster orderMaster) {
                        return new Tuple2<>("cancelCount", 1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(1);

        SingleOutputStreamOperator<String> cancelRate = (SingleOutputStreamOperator<String>) orderCounts
                .join(cancelCounts)
                .where(i -> true)
                .equalTo(i -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply((cancelCount, orderCount) -> {
                    double rate = ((cancelCount.f1 * 1.0) / (orderCount.f1 * 1.0));
                    String s = String.format("%.1f", rate);
                    return s + "%";
                });

        dwdData.map(i->"totalcount:"+i).print();
        refundSum.map(i->"refundcountminute:"+i).print();
        cancelRate.map(i->"cancelrate:"+i).print();

//        dwdData.addSink(new RedisWriter().redisSet("totalcount"));
//        refundSum.addSink(new RedisWriter().redisSet("refundcountminute"));
//        cancelRate.addSink(new RedisWriter().redisSet("cancelrate"));
        env.execute("countRateRedis");

    }


}
