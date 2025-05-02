package org.example.gs7;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.*;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class countTopRedisJoinHbase implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ods_mall_data";
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        OutputTag<Tuple2<String, Double>[]> top3itemconsumptionlist = new OutputTag<Tuple2<String, Double>[]>("top3itemconsumptionlist") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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

//        data.addSink(new RedisWriter().redisSet("totalcount"));
//        data.print();
        SingleOutputStreamOperator<String> top3itemconsumption = data.getSideOutput(top3itemconsumptionlist).map(tuple2s -> {
            String s = "";
            for (int i = 0; i < tuple2s.length; i++) {
                String s1 = "";
                if (i < tuple2s.length-1) s1 = ",";
                s = s + tuple2s[i].f0 + ":" + tuple2s[i].f1 + s1;
            }
            return "top3itemconsumption:[" + s + "]";
        });
//        top3itemconsumption.print();
//        top3itemconsumption.addSink(new RedisWriter().redisSet("top3itemconsumption"));

        SingleOutputStreamOperator<OdsMallLog> odsMallLogStream = env
                .readTextFile("E:\\input\\ods_mall_log.txt")
                .map(new MapFunction<String, OdsMallLog>() {
                    @Override
                    public OdsMallLog map(String value) throws Exception {
                        // 使用正则表达式解析文本行
                        Pattern pattern = Pattern.compile("([a-zA-Z_]+):\\(([^)]+)\\);");
                        Matcher matcher = pattern.matcher(value);

                        if (matcher.find()) {
                            String tableName = matcher.group(1);
                            String data = matcher.group(2);
                            return new OdsMallLog(tableName, data);
                        } else {
                            // 如果没有匹配，可以抛出异常或者返回null
                            throw new IllegalArgumentException("Invalid log format: " + value);
                        }
                    }
                });

        odsMallLogStream.map(odsMallLog -> odsMallLog.getTable()).print();
        env.execute("countRedisMysql");
    }
}
