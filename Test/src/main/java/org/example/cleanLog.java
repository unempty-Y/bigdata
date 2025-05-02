package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class cleanLog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "ods_mall_log";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<OdsMallLog> odsMallLogSingleOutputStreamOperator = env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, OdsMallLog>() {
                    @Override
                    public OdsMallLog map(String s) throws Exception {
                        String[] split = s.split(":");
                        return new OdsMallLog(split[0], split[1]);
                    }
                });
        SingleOutputStreamOperator<ProductBrowse> productBrowse = odsMallLogSingleOutputStreamOperator.filter(odsMallLog -> odsMallLog.getTable().equals("product_browse"))
                .map(new MapFunction<OdsMallLog, ProductBrowse>() {
                    @Override
                    public ProductBrowse map(OdsMallLog odsMallLog) throws Exception {
                        String data = odsMallLog.getData();
                        String substring = data.substring(1, data.length() - 2);
                        String[] split = substring.split("\\|");
                        int ran = new Random().nextInt(10);
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMddHHmmssSSS");
                        String date = simpleDateFormat.format(new Date());
                        String log_id = ran + date;
                        return new ProductBrowse(log_id,
                                split[0],
                                Integer.parseInt(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                split[4].substring(1, split[4].length() - 1));
                    }
                });
        SingleOutputStreamOperator<String> projson = productBrowse.map(new MapFunction<ProductBrowse, String>() {
            @Override
            public String map(ProductBrowse productBrowse) throws Exception {
                Gson gson = new Gson();
                return gson.toJson(productBrowse);
            }
        });
//        projson.print();
        projson.sinkTo(new KafkaConnect().kafkaWriter("log_product_browse"));
        SingleOutputStreamOperator<Tuple2<String, Tuple3<String, String, String>[]>> toHbase = productBrowse
                .map(new MapFunction<ProductBrowse, Tuple2<String, Tuple3<String, String, String>[]>>() {
                    @Override
                    public Tuple2<String, Tuple3<String, String, String>[]> map(ProductBrowse productBrowse) throws Exception {
                        //创建随机数rowkey
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
                        String year = simpleDateFormat.format(new Date());
                        String ran = productBrowse.getLog_id().substring(0, 1);
                        String date = productBrowse.getLog_id().substring(1);
                        String rowKey = ran+year+date;
                        //获取列名和值
                        Field[] fields = ProductBrowse.class.getDeclaredFields();
                        int length = fields.length;
                        String[] fieldNames = new String[length];
                        String[] values = new String[length];
                        for (int i = 0; i < length; i++) {
                            fields[i].setAccessible(true); // 如果字段是私有的
                            fieldNames[i] = fields[i].getName();
                            values[i] = fields[i].get(productBrowse).toString();
                        }
                        Tuple3<String, String, String>[] tuples = new Tuple3[length];
                        for (int i = 0; i < length; i++) {
                            tuples[i] = new Tuple3<>("info", fieldNames[i], values[i]);
                        }
                        return new Tuple2<>(rowKey, tuples);
                    }
                });
        toHbase.print();
        toHbase.addSink(new HbaseWriter().hbaseWriter("ods:product_browse"));
        env.execute();
    }
}
