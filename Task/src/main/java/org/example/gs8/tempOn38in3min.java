package org.example.gs8;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.EnvironmentData;
import org.example.KafkaConnect;
import org.example.RedisWriter;

import java.text.SimpleDateFormat;

public class tempOn38in3min {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "EnvironmentData";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<EnvironmentData> environmentDataSingleOutputStreamOperator = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, EnvironmentData>() {
                    @Override
                    public EnvironmentData map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new EnvironmentData(
                                Integer.parseInt(split[0]),
                                Integer.parseInt(split[1]),
                                Integer.parseInt(split[2]),
                                Integer.parseInt(split[3]),
                                Integer.parseInt(split[4]),
                                Double.parseDouble(split[5]),
                                Double.parseDouble(split[6]),
                                Integer.parseInt(split[7]),
                                Integer.parseInt(split[8]),
                                Integer.parseInt(split[9]),
                                split[10]
                        );
                    }
                });
//        environmentDataSingleOutputStreamOperator.print();
        KeyedProcessFunction<Integer, EnvironmentData, Tuple2<String, String>> lastTime = new KeyedProcessFunction<Integer, EnvironmentData, Tuple2<String, String>>() {
            private ValueState<Tuple3<Integer, String, Boolean>> lastTime;

            @Override
            public void open(Configuration parameters) throws Exception {
                //BaseID，这一段3分钟内连续高于38度最早的时间，上一次是否高于38度
                lastTime = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<Integer, String, Boolean>>("lastTime", TypeInformation.of(new TypeHint<Tuple3<Integer, String, Boolean>>() {
                })));
            }

            @Override
            public void processElement(EnvironmentData environmentData, KeyedProcessFunction<Integer, EnvironmentData, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                if (lastTime.value() == null)
                    lastTime.update(new Tuple3(environmentData.getBaseID(), environmentData.getInPutTime(), false));//初始化数据
                Tuple3<Integer, String, Boolean> value = lastTime.value();
                if (environmentData.getTemperature() > 38.0) {
                    //如果上次温度高于38
                    if (value.f2) {
                        long l = dateFormat.parse(environmentData.getInPutTime()).getTime() - dateFormat.parse(value.f1).getTime();
                        if (l > 180000) {
                            value.f1 = environmentData.getInPutTime();
                            collector.collect(
                                    new Tuple2<>(environmentData.getBaseID()+"", environmentData.getBaseID() + "-" +
                                            environmentData.getInPutTime()+"设备" +
                                            environmentData.getBaseID() + "连续三分钟温度高于38度请及时处理！"));
                        }
                    } else
                        value.f1 = environmentData.getInPutTime();
                    value.f2 = true;
                }
                lastTime.update(value);
            }
        };
        SingleOutputStreamOperator<Tuple2<String, String>> warningTime = environmentDataSingleOutputStreamOperator
                .keyBy(environmentData -> environmentData.getBaseID())
                .process(lastTime);
        warningTime.print();
        warningTime.addSink(new RedisWriter().redisHSet("env_temperature_monitor"));
        env.execute("tempOn38in3min");
    }
}
