package xl_ds

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.hbase.thirdparty.com.google.gson.JsonParser
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.json.JSONObject
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson._
import org.json4s.JsonDSL._

import java.util.Properties

object ds_2_1 {
  val MaxOutOfOrderness: Long = 1000L

  def main(args: Array[String]): Unit = {


    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置使用处理时间
    import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置并行度
    env.setParallelism(1)

    // 启用检查点
    env.enableCheckpointing(5000)

    // kafka source
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers("192.168.45.16:9092")
      .setTopics("ods_mall_data")
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      //      .setGroupId("group-test")
      .setStartingOffsets(OffsetsInitializer.earliest)
//      .setStartingOffsets(OffsetsInitializer.)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    // kafka sink
    val properties = new Properties()
    properties.setProperty("trans.timeout.ms", "7200000") // 2 hours

    // KafkaSink 允许将记录流写入一个或多个 Kafka 主题。
    val kafkaSink = KafkaSink.builder[String]
      .setBootstrapServers("192.168.45.16:9092")
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String] //.builder()
        .setTopicSelector(new TopicSelector[String] {
          override def apply(t: String): String = {
            if (t.contains("order_id")) "fact_order_master"
            else if (t.contains("order_detail")) "fact_order_detail"
            else if (t.contains("customer_info")) "dim_customer_info"
            else if (t.contains("product_info")) "dim_product_info"
            else null
          }
        })
        .setValueSerializationSchema(new SimpleStringSchema)
        .build()).build()

    val dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line =>
        line.contains("order_master") ||
          line.contains("order_detail") ||
          line.contains("customer_info") || line.contains("product_info")
      )
//      .map(line => {
//        import com.google.gson.JsonParser
//        val jsonobj = new JsonParser().parse(line).getAsJsonObject
//        jsonobj.getAsJsonObject("data").toString
//      })
//      .map(line=>s"data:$line")
//    dataStream.print()
    val fact_order_detail = dataStream.filter(line=>line.contains("order_id"))
      .map(line=> {
        val str: JValue = JsonMethods.parse(line, useBigDecimalForDouble = true)
        val JString(modified_time) = (str \\ "modified_time")
        modified_time
      })
//      .assignTimestampsAndWatermarks(new OrderTSExtractor)
//      .map((1,_))
//      .keyBy(_._1)
//      .process(new MyOrderProcessFunction)
//      .map(line=>{
//val arr = line.stripPrefix("{").stripPrefix("}").split(",")
//        arr(0)})
      fact_order_detail.print
//      dataStream.sinkTo(kafkaSink)
    env.execute("Task1")

  }
  class OrderTSExtractor extends BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(MaxOutOfOrderness)) {
    // 抽取时间戳
    def extractTimestamp(order: String): Long = {
      val str: JValue = JsonMethods.parse(order, useBigDecimalForDouble = true)
      (str \ "modified_time").toString.toLong
    }
  }
  class MyOrderProcessFunction extends KeyedProcessFunction[Int, ((Int,String)), String] {
    override def processElement(input:((Int,String)),
                                context: KeyedProcessFunction[Int, ((Int,String)), String]#Context,
                                out: Collector[String]): Unit = {
        out.collect(("top2userconsumption"))
    }
  }
}