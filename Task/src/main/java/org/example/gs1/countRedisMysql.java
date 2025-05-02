package org.example.gs1;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;

public class countRedisMysql implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ods_mall_data";
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        OutputTag<Double> totalrefundordercount = new OutputTag<Double>("totalrefundordercount") {
        };//测流标签
        OutputTag<OrderMaster> MysqlData = new OutputTag<OrderMaster>("MysqlData") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ProcessFunction<OrderMaster, Double> processFunction = new ProcessFunction<OrderMaster, Double>() {

            private transient ValueState<Double> paymoneyCount;
            private transient ValueState<Double> refundPaymoneyCount;

            @Override
            public void open(Configuration parameters) throws Exception {
                paymoneyCount = getRuntimeContext().getState(new ValueStateDescriptor<Double>("PriceCount", Double.class));
                refundPaymoneyCount = getRuntimeContext().getState(new ValueStateDescriptor<Double>("refundPaymoneyCount", Double.class));
            }
            @Override
            public void processElement(OrderMaster orderMaster, ProcessFunction<OrderMaster, Double>.Context context, Collector<Double> collector) throws Exception {
                if (Arrays.asList(filter).contains(orderMaster.getOrder_status())) {
                    Double value = paymoneyCount.value();
                    if (value == null) value = 0.0;//开始为空
                    double tmp = value + orderMaster.getPayment_money();
                    paymoneyCount.update(tmp);
                    collector.collect(paymoneyCount.value());
                } else if (orderMaster.getOrder_status().equals("已退款")) {
                    Double value = refundPaymoneyCount.value();
                    if (value == null) value = 0.0;
                    double tmp = value + orderMaster.getPayment_money();
                    refundPaymoneyCount.update(tmp);
                    context.output(totalrefundordercount, refundPaymoneyCount.value());
                } else if (orderMaster.getOrder_status().equals("已下单"))
                    context.output(MysqlData, orderMaster);
            }
        };
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);

        WatermarkStrategy<OrderMaster> watermarkStrategy =WatermarkStrategy.<OrderMaster>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderMaster>() {
                    @Override
                    public long extractTimestamp(OrderMaster orderMaster, long l) {
                        try {
                            return timeFormat.parse(orderMaster.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        SingleOutputStreamOperator<OdsMallData> odsMallDataStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
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
                .assignTimestampsAndWatermarks(watermarkStrategy);

        SingleOutputStreamOperator<Double> data = orderMasterStream
                .keyBy(order_master -> true)
                .process(processFunction);//一定不要再用map，不然侧边流会得不到数据

        SingleOutputStreamOperator<String> totalPrice = data.map(i -> String.format("%.2f", i));
        totalPrice.addSink(new RedisWriter().redisSet("totalprice"));
        totalPrice.map(i->"totalprice:"+i).print();

        DataStream<String> totalRefundordercount = data.getSideOutput(totalrefundordercount)
                .map(i->String.format("%.2f", i));
        totalRefundordercount.map(i->"totalrefundordercount:"+i).print();
        totalRefundordercount.addSink(new RedisWriter().redisSet("totalrefundordercount"));

        DataStream<Object> toMysql = data.getSideOutput(MysqlData).map(new MapFunction<OrderMaster, Object>() {
            @Override
            public Object map(OrderMaster orderMaster) throws Exception {
                return orderMaster;
            }
        });
        toMysql.print();
        toMysql.addSink(new JdbcWriter().JdbcWriter("mysql", "shtd_result", "order_info"));

        env.execute("countRedisMysql");
    }
}
