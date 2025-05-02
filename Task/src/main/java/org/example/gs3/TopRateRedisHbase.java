package org.example.gs3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;

public class TopRateRedisHbase implements Constants {
    static String topic = "ods_mall_data";

    public static void main(String[] args) throws Exception {
        OutputTag<OrderMaster> cancel = new OutputTag<OrderMaster>("cancel") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        WatermarkStrategy<OrderMaster> orderMasterWatermarkStrategy = new Watermarks().orderMasterWatermarkStrategy();

        ProcessFunction<OrderMaster, String> processFunction = new ProcessFunction<OrderMaster, String>() {
            @Override
            public void processElement(OrderMaster orderMaster, ProcessFunction<OrderMaster, String>.Context context, Collector<String> collector) throws Exception {
                if (Arrays.asList(filter).contains(orderMaster.getOrder_status())) {
                    TopN topN = new TopN();
                    String member = orderMaster.getCustomer_id() + ":" + orderMaster.getShipping_user();
                    topN.addElem("userconsumption", orderMaster.getPayment_money(), member);
                    Tuple2<String, Double>[] userconsumptions = topN.getTopN("userconsumption", 0, 2, true);
                    String put = "[" + userconsumptions[0].f0 + ":" + userconsumptions[0].f1 + "," + userconsumptions[1].f0 + ":" + userconsumptions[1].f1 + "]";
                    collector.collect(put);
                } else if (orderMaster.getOrder_status().equals("已退款")) context.output(cancel, orderMaster);
            }
        };

        SingleOutputStreamOperator<OdsMallData> odsMallDataStream = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .readTextFile("E:\\input\\order_master.txt")
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
        SingleOutputStreamOperator<String> top2user = order_master
                .keyBy(i -> true)
                .process(processFunction);

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

        DataStream<OrderMaster> cancelOrders = top2user.getSideOutput(cancel);

        SingleOutputStreamOperator<Tuple2<String, Integer>> cancelCounts = cancelOrders
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
        SingleOutputStreamOperator<Tuple2<String, Tuple3<String, String, String>[]>> cancelHbase = cancelOrders
                .map(i -> {
                    int num = new Random().nextInt(10);
                    Field[] fields = OrderMaster.class.getDeclaredFields();
                    String[] listName = new String[fields.length];
                    String[] value = new String[fields.length];
                    for (int j = 0; j < fields.length; j++) {
                        fields[j].setAccessible(true);
                            listName[j] = fields[j].getName();
                            value[j] = fields[j].get(i).toString();
                    }
                    Tuple3<String, String, String>[] tuples = new Tuple3[listName.length];
                    for (int j = 0; j < fields.length; j++) {
                        tuples[j] = new Tuple3<>("info", listName[j], value[j]);
                    }
                    return new Tuple2<>(num + i.getCreate_time(), tuples);
                });
        top2user.addSink(new RedisWriter().redisSet("top2userconsumption"));
        cancelRate.addSink(new RedisWriter().redisSet("cancelrate"));
        cancelHbase.addSink(new HbaseWriter().hbaseWriter("shtd_result:order_info"));
        env.execute("TopRateRedisHbase");
    }

}