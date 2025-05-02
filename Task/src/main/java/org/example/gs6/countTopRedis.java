package org.example.gs6;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.*;

import java.text.SimpleDateFormat;
import java.util.Arrays;

public class countTopRedis implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ods_mall_data";
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        OutputTag<String> top3itemamountlist = new OutputTag<String>("top3itemamountlist") {
        };
        OutputTag<Tuple2<String, Double>[]> top3itemconsumptionlist = new OutputTag<Tuple2<String, Double>[]>("top3itemconsumptionlist") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String[] cleans = new String[]{"top3itemamountlist", "top3itemconsumptionlist"};
        ProcessFunction<OrderMaster, String> processFunction = new ProcessFunction<OrderMaster, String>() {
            private transient ValueState<Integer> priceCount;

            @Override
            public void open(Configuration parameters) throws Exception {
                priceCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("PriceCount", Integer.class));
            }

            @Override
            public void processElement(OrderMaster ordermaster, ProcessFunction<OrderMaster, String>.Context context, Collector<String> collector) throws Exception {
                if (Arrays.asList(filter).contains(ordermaster.getOrder_status())) {
                    Integer value = priceCount.value();
                    if (value == null) value = 0;
                    int tmp = value + 1;
                    priceCount.update(tmp);
                    collector.collect(priceCount.value().toString());
                    TopN topN = new TopN();
                    topN.addElem("top3itemamountlist", 1, ordermaster.getOrder_id() + "");
                    Tuple2<String, Double>[] top3itemamountliststmp = topN.getTopN("top3itemamountlist", 0, 3);
                    Tuple2<String, Integer>[] top3itemamountlists = new Tuple2[top3itemamountliststmp.length];
                    for (int i = 0; i < top3itemamountliststmp.length; i++) {
                        top3itemamountlists[i] = new Tuple2(top3itemamountliststmp[i].f0, top3itemamountliststmp[i].f1);
                    }
                    String s = "";
                    for (int i = 0; i < top3itemamountlists.length; i++) {
                        String s1 = "";
                        if (i < top3itemamountlists.length-1) s1 = ",";
                        s = s + top3itemamountlists[i].f0 + ":" + top3itemamountlists[i].f1 + s1;
                    }
                    String tupleTopToString = "top3itemamount:[" + s + "]";
                    context.output(top3itemamountlist, tupleTopToString);
                    topN.addElem("top3itemconsumptionlist", ordermaster.getPayment_money(), ordermaster.getOrder_id() + "");
                    Tuple2<String, Double>[] top3itemconsumptionlists = topN.getTopN("top3itemconsumptionlist", 0, 3);
                    context.output(top3itemconsumptionlist, top3itemconsumptionlists);
                }
            }
        };
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);

        WatermarkStrategy<OrderMaster> orderMasterWatermarkStrategy = new Watermarks().orderMasterWatermarkStrategy();
        SingleOutputStreamOperator<OdsMallData> odsMallDataStream = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .readTextFile("E:\\input\\order_master.txt")
                .map(in -> {
                    Gson gson = new GsonBuilder()
                            .create();
                    OdsMallData odsMallData = gson.fromJson(in, OdsMallData.class);
                    return odsMallData;
                });
        SingleOutputStreamOperator<OrderMaster> orderMasterStream = odsMallDataStream
                .filter(i -> i.getTable().equals("order_master"))
                .map(i -> {
                    Gson gson = new Gson();
                    String json = gson.toJson(i.getData());
                    OrderMaster orderMaster = gson.fromJson(json, OrderMaster.class);
                    return orderMaster;
                })
                .assignTimestampsAndWatermarks(orderMasterWatermarkStrategy);

        SingleOutputStreamOperator<String> data = orderMasterStream
                .keyBy(order_master -> true)
                .process(processFunction);
//        data.addSink(new RedisWriter().redisSet("totalprice"));
        data.print();
//        orderMasterStream.print();
        DataStream<String> top3itemamount = data.getSideOutput(top3itemamountlist);
//        top3itemamount.addSink(new RedisWriter().redisSet("top3itemamount"));
        top3itemamount.print();

        SingleOutputStreamOperator<String> top3itemconsumption = data.getSideOutput(top3itemconsumptionlist).map(tuple2s -> {
            String s = "";
            for (int i = 0; i < tuple2s.length; i++) {
                String s1 = "";
                if (i < tuple2s.length-1) s1 = ",";
                s = s + tuple2s[i].f0 + ":" + tuple2s[i].f1 + s1;
            }
            return "top3itemconsumption:[" + s + "]";
        });
//        top3itemconsumption.addSink(new RedisWriter().redisSet("top3itemconsumption"));

        env.execute("countRedisMysql");
    }
}
