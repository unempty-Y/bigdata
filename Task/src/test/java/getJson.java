import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.Constants;

/**
 * 这个类用于从Kafka读取JSON格式的数据。
 *
 * @author 您的名字
 * @date 2024/06/07
 */
public class getJson implements Constants {
    public static void main(String[] args) throws Exception {
        String topic = "ods_mall_data";
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bigdata1 + ":" + kafkaPort)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), topic);
        stringDataStreamSource
                .writeAsText("E:\\input\\order_master4.txt");
        stringDataStreamSource.print();
        env.execute("getJson");
    }
}
