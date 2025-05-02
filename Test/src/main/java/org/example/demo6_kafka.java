package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class demo6_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        en.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.45.16:9092")
                .setTopics("ProduceRecord")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafka_source = en.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
        KafkaSink<String> kafkasinkmain = KafkaSink.<String>builder().setBootstrapServers("192.168.45.16:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("main_kafka").setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).build();

        KafkaSink<String> kafkasinkside = KafkaSink.<String>builder().setBootstrapServers("192.168.45.16:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("side_kafka").setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).build();
        OutputTag<String> outputTag  = new OutputTag<String>("side_output"){};
        SingleOutputStreamOperator<String> mainstream = kafka_source.process(new ProcessFunction<String, String>() {

            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                if (s.split(",")[9].equals("0")) {
                    collector.collect(s.substring(0,s.length()-2));
                } else {
                    context.output(outputTag, s.substring(0,s.length()-2));
                }

            }
        });
        DataStream<String> sideOutput = mainstream.getSideOutput(outputTag);

        SingleOutputStreamOperator<String> mainmap  = mainstream.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        SingleOutputStreamOperator<String> sidemap = sideOutput.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });


        mainmap.sinkTo(kafkasinkmain);
        sidemap.sinkTo(kafkasinkside);


        en.execute("aa");

    }
}
