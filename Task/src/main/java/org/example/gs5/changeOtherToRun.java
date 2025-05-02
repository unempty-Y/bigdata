package org.example.gs5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.ChangeRecord;
import org.example.Constants;
import org.example.JdbcWriter;
import org.example.KafkaConnect;

import java.text.SimpleDateFormat;
import java.util.Date;

public class changeOtherToRun implements Constants {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        String topic = "ChangeRecord";
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        SingleOutputStreamOperator<ChangeRecord> changeRecordSingleOutputStreamOperator = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(new MapFunction<String, ChangeRecord>() {
                    @Override
                    public ChangeRecord map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new ChangeRecord(
                                split[0],
                                Integer.parseInt(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                split[4],
                                split[5],
                                Integer.parseInt(split[6])
                        );
                    }
                });
        KeyedProcessFunction<Integer, ChangeRecord, ChangeStateOtherToRunAgg> lastState = new KeyedProcessFunction<Integer, ChangeRecord, ChangeStateOtherToRunAgg>() {
            private ValueState<ChangeStateOtherToRunAgg> lastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastState = getRuntimeContext().getState(new ValueStateDescriptor<ChangeStateOtherToRunAgg>("lastState", ChangeStateOtherToRunAgg.class));
            }

            @Override
            public void processElement(ChangeRecord changeRecord, KeyedProcessFunction<Integer, ChangeRecord, ChangeStateOtherToRunAgg>.Context context, Collector<ChangeStateOtherToRunAgg> collector) throws Exception {
                //初始数据，在这里把intime借用来存储上次的状态，1为运行
                ChangeStateOtherToRunAgg tmp = new ChangeStateOtherToRunAgg(changeRecord.getChangeMachineID(), "null", 0, "1");
                if (lastState.value() == null) lastState.update(tmp);
                tmp = lastState.value();
                System.out.print(changeRecord.getChangeRecordState());
                if (changeRecord.getChangeRecordState().equals("运行")) {
                    if (tmp.getIn_time().equals("0")) {
                        tmp.setTotal_change_torun(tmp.getTotal_change_torun() + 1);
                        tmp.setIn_time("1");
                    }
                } else {
                    tmp.setLast_machine_state(changeRecord.getChangeRecordState());
                    tmp.setIn_time("0");
                }
                lastState.update(tmp);
                collector.collect(tmp);
            }
        };
        SingleOutputStreamOperator<ChangeStateOtherToRunAgg> changeStateOtherToRunAggSingleOutputStreamOperator = changeRecordSingleOutputStreamOperator
                .keyBy(changeRecord -> changeRecord.getChangeMachineID())
                .process(lastState)
                .filter(changeStateOtherToRunAgg -> !changeStateOtherToRunAgg.getLast_machine_state().equals("null"))
                .map(new MapFunction<ChangeStateOtherToRunAgg, ChangeStateOtherToRunAgg>() {
                    @Override
                    public ChangeStateOtherToRunAgg map(ChangeStateOtherToRunAgg changeStateOtherToRunAgg) throws Exception {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String date = dateFormat.format(new Date());
                        changeStateOtherToRunAgg.setIn_time(date);
                        return changeStateOtherToRunAgg;
                    }
                });
        changeStateOtherToRunAggSingleOutputStreamOperator.print();
        changeStateOtherToRunAggSingleOutputStreamOperator
                .map(new MapFunction<ChangeStateOtherToRunAgg, Object>() {
                    @Override
                    public Object map(ChangeStateOtherToRunAgg changeStateOtherToRunAgg) throws Exception {
                        return changeStateOtherToRunAgg;
                    }
                }).addSink(new JdbcWriter().JdbcWriter("mysql","shtd_industry","change_state_other_to_run_agg"));
        env.execute("changeOtherToRun");
    }
}
