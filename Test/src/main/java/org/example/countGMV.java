package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import redis.clients.jedis.Jedis;

public class countGMV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "fact_order_detail";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<OrderDetail> orderDetailSingleOutputStreamOperator = env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, OrderDetail>() {
                    @Override
                    public OrderDetail map(String s) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(s, OrderDetail.class);
                    }
                });
        SingleOutputStreamOperator<Double> GMV = orderDetailSingleOutputStreamOperator
                .map(new MapFunction<OrderDetail, Double>() {
                    @Override
                    public Double map(OrderDetail orderDetail) throws Exception {
                        return orderDetail.getProduct_cnt() * orderDetail.getProduct_price();
                    }
                }).keyBy(i -> true)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .sum(0);
        GMV.addSink(new RichSinkFunction<Double>() {
            Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("bigdata1", 6379);
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }

            @Override
            public void invoke(Double value, Context context) throws Exception {
                String format = String.format("%.2f", value);
                jedis.set("store_gmv", format);
            }
        });
        GMV.print();
        env.execute("countGMV");
    }
}
