package test

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.Properties


object task1 {

  def main(args: Array[String]): Unit = {

    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置使用处理时间
    import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 设置并行度
    env.setParallelism(1)

    // 启用检查点
    env.enableCheckpointing(5000)

    // kafka source
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers("192.168.45.21:9092")
      .setTopics("ods_mall_data")
      .setGroupId("group-test")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    // kafka sink
    val properties = new Properties()
    properties.setProperty("trans.timeout.ms", "7200000") // 2 hours

    // KafkaSink 允许将记录流写入一个或多个 Kafka 主题。
    val kafkaSink = KafkaSink.builder[String]
      .setBootstrapServers("bigdata1:9092")
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String] //.builder()
        .setTopicSelector(new TopicSelector[String] {
          override def apply(t: String): String = {
            if (t.contains("order_id")) "fact_order_master"
            else if (t.contains("order_detail")) "fact_order_detail"
            else null
          }
        })
        .setValueSerializationSchema(new SimpleStringSchema)
        .build()).build()

    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line => line.contains("order_master") || line.contains("order_detail"))
      .map(line => {
        import com.google.gson.JsonParser
        val jsonobj = new JsonParser().parse(line).getAsJsonObject
        jsonobj.getAsJsonObject("data").toString
      })
      //      .print
      .sinkTo(kafkaSink)
    /* .map(line => {
       val jsonObj = JsonParser.parseString(line).getAsJsonObject.getAsJsonObject("data")
       jsonObj.toString
     })
     // .print
     .sinkTo(kafkaSink)*/
    env.execute("Task1")
  }
}