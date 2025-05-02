package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class cleanData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "ods_mall_data";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<OdsMallData> odsMallDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, OdsMallData>() {
                    @Override
                    public OdsMallData map(String s) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(s, OdsMallData.class);
                    }
                });
        SingleOutputStreamOperator<OrderMaster> orderMasterStream = odsMallDataStream.filter(odsMallData -> odsMallData.getTable().equals("order_master"))
                .map(new MapFunction<OdsMallData, OrderMaster>() {
                    @Override
                    public OrderMaster map(OdsMallData odsMallData) throws Exception {
                        Gson gson = new Gson();
                        String json = gson.toJson(odsMallData.getData());
                        return gson.fromJson(json, OrderMaster.class);
                    }
                });
        SingleOutputStreamOperator<OrderDetail> orderDetailStream = odsMallDataStream.filter(odsMallData -> odsMallData.getTable().equals("order_detail"))
                .map(new MapFunction<OdsMallData, OrderDetail>() {
                    @Override
                    public OrderDetail map(OdsMallData odsMallData) throws Exception {
                        Gson gson = new Gson();
                        String json = gson.toJson(odsMallData.getData());
                        return gson.fromJson(json, OrderDetail.class);
                    }
                });
        orderMasterStream
                .map(new MapFunction<OrderMaster, String>() {
                    @Override
                    public String map(OrderMaster orderMaster) throws Exception {
                        return new Gson().toJson(orderMaster);
                    }
                })
                .sinkTo(new KafkaConnect().kafkaWriter("fact_order_master"));
        orderDetailStream
                .map(new MapFunction<OrderDetail, String>() {
                    @Override
                    public String map(OrderDetail orderDetail) throws Exception {
                        return new Gson().toJson(orderDetail);
                    }
                })
                .sinkTo(new KafkaConnect().kafkaWriter("fact_order_detail"));
        SingleOutputStreamOperator<Tuple2<String, Tuple3<String, String, String>[]>> masterHbase = orderMasterStream.map(new MapFunction<OrderMaster, Tuple2<String, Tuple3<String, String, String>[]>>() {
            @Override
            public Tuple2<String, Tuple3<String, String, String>[]> map(OrderMaster orderMaster) throws Exception {
                //创建随机数rowkey
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                String date = simpleDateFormat.format(new Date());
                int ran = new Random().nextInt(10);
                String rowKey = ran + date;
                //获取列名和值
                Field[] fields = OrderMaster.class.getDeclaredFields();
                int length = fields.length;
                String[] fieldNames = new String[length];
                String[] values = new String[length];
                for (int i = 0; i < length; i++) {
                    fields[i].setAccessible(true); // 如果字段是私有的
                    fieldNames[i] = fields[i].getName();
                    values[i] = fields[i].get(orderMaster).toString();
                }
                Tuple3<String, String, String>[] tuples = new Tuple3[length];
                for (int i = 0; i < length; i++) {
                    tuples[i] = new Tuple3<>("info", fieldNames[i], values[i]);
                }
                return new Tuple2<>(rowKey, tuples);

            }
        });
        SingleOutputStreamOperator<Tuple2<String, Tuple3<String, String, String>[]>> detailHbase = orderDetailStream.map(new MapFunction<OrderDetail, Tuple2<String, Tuple3<String, String, String>[]>>() {
            @Override
            public Tuple2<String, Tuple3<String, String, String>[]> map(OrderDetail orderDetail) throws Exception {
                //创建随机数rowkey
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                String date = simpleDateFormat.format(new Date());
                int ran = new Random().nextInt(10);
                String rowKey = ran + date;
                //获取列名和值
                Field[] fields = OrderDetail.class.getDeclaredFields();
                int length = fields.length;
                String[] fieldNames = new String[length];
                String[] values = new String[length];
                for (int i = 0; i < length; i++) {
                    fields[i].setAccessible(true); // 如果字段是私有的
                    fieldNames[i] = fields[i].getName();
                    values[i] = fields[i].get(orderDetail).toString();
                }
                Tuple3<String, String, String>[] tuples = new Tuple3[length];
                for (int i = 0; i < length; i++) {
                    tuples[i] = new Tuple3<>("info", fieldNames[i], values[i]);
                }
                return new Tuple2<>(rowKey, tuples);
            }
        });
//        masterHbase.addSink(new HbaseWriter().hbaseWriter("ods:order_master"));
//        detailHbase.addSink(new HbaseWriter().hbaseWriter("ods:order_detail"));
        env.execute("cleanData");
    }
}
