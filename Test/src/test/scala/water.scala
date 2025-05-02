//import org.apache.flink.api.common.eventtime.{Watermark, WatermarkStrategy}
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.scala._
//import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
//import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import org.json4s._
//import org.json4s.jackson._
//
//import java.sql.PreparedStatement
//import java.text.SimpleDateFormat
//import java.util.Properties
//
//object water {
//  val MaxOutOfOrderness: Long = 2*1000L
//
//  def main(args: Array[String]): Unit = {
//
//
//    // 设置流执行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // 设置使用处理时间
//    import org.apache.flink.streaming.api.TimeCharacteristic
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    // 设置并行度
//    env.setParallelism(1)
//
//    // 启用检查点
//    env.enableCheckpointing(5000)
//
//    // kafka source
//    val kafkaSource = KafkaSource.builder[String]
//      .setBootstrapServers("192.168.45.16:9092")
//      .setTopics("ods_mall_data")
//      .setStartingOffsets(OffsetsInitializer.committedOffsets())
//      //      .setGroupId("group-test")
//      .setStartingOffsets(OffsetsInitializer.earliest)
//      .setValueOnlyDeserializer(new SimpleStringSchema)
//      .build()
//
//    // kafka sink
//    val properties = new Properties()
//    properties.setProperty("trans.timeout.ms", "7200000") // 2 hours
//
//    // KafkaSink 允许将记录流写入一个或多个 Kafka 主题。
//    val kafkaSink = KafkaSink.builder[String]
//      .setBootstrapServers("192.168.45.16:9092")
//      .setKafkaProducerConfig(properties)
//      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String] //.builder()
//        .setTopicSelector(new TopicSelector[String] {
//          override def apply(t: String): String = {
//            if (t.contains("order_id")) "fact_order_master"
//            else if (t.contains("order_detail")) "fact_order_detail"
//            else if (t.contains("customer_info")) "dim_customer_info"
//            else if (t.contains("product_info")) "dim_product_info"
//            else null
//          }
//        })
//        .setValueSerializationSchema(new SimpleStringSchema)
//        .build()).build()
//    val insetIntoCkSql = "insert into order_master (data) values (?)"
//
//    val ckSink: SinkFunction[String] = JdbcSink.sink(
//      //插入数据SQL
//      insetIntoCkSql,
//
//      //设置插入ClickHouse数据的参数
//      new JdbcStatementBuilder[String] {
//        override def accept(ps: PreparedStatement, tp: String): Unit = {
//          ps.setString(1, tp)
//        }
//      },
//      //设置批次插入数据
//      new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
//
//      //设置连接ClickHouse的配置
//      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
//        .withUrl("jdbc:clickhouse://192.168.45.16:8123/shtd_result")
//        .withUsername("default")
//        .withPassword("123456")
//        .build()
//    )
//    val dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
////      .filter(line =>
////        line.contains("order_master") ||
////          line.contains("order_detail") ||
////          line.contains("customer_info") || line.contains("product_info")
////      )
////      .map(line => {
////        import com.google.gson.JsonParser
////        val jsonobj = new JsonParser().parse(line).getAsJsonObject
////        jsonobj.getAsJsonObject("data").toString
////      })
////      .map(line=>s"data:$line")
////    dataStream.print()
//val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//
//    val fact_order_detail = dataStream
//      .filter(line=>line.contains("order_master"))
//      .map(line=> {
//        val str: JValue = JsonMethods.parse(line, useBigDecimalForDouble = true)
//        val JString(modified_time) = (str \\ "modified_time")
//        (modified_time,fm.parse(modified_time).getTime)
////        (line)
//      })
//
//      .assignTimestampsAndWatermarks(new OrderTSExtractor)
//      .keyBy(_=>true) // 以某种键进行分组
//      .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 定义5分钟的窗口
//      .trigger(EventTimeTrigger.create()) // 使用EventTime触发器
//      .apply((key: Boolean, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]) => {
//        // 对窗口内的数据按时间戳进行排序
//        val sortedInput = input.toList
//        // 发出按时间顺序排序后的数据
//        sortedInput.foreach { case (line, timestamp) =>
//          out.collect(line)
//        }
//      })
////      .map((1,_))
////      .keyBy(_._1)
////      .process(new MyOrderProcessFunction)
////      .map(line=>{
////val arr = line.stripPrefix("{").stripPrefix("}").split(",")
////        arr(0)})
//      fact_order_detail.print
////    fact_order_detail.addSink(ckSink)
////      dataStream.sinkTo(kafkaSink)
//    env.execute("Task1")
//
//  }
//  class OrderTSExtractor extends BoundedOutOfOrdernessTimestampExtractor[(String,Long)](Time.minutes(1)) {
//    // 抽取时间戳
//    def extractTimestamp(order: (String,Long)): Long = order._2
//  }
//  class MyOrderProcessFunction extends KeyedProcessFunction[Int, ((Int,String)), String] {
//    override def processElement(input:((Int,String)),
//                                context: KeyedProcessFunction[Int, ((Int,String)), String]#Context,
//                                out: Collector[String]): Unit = {
//        out.collect(("top2userconsumption"))
//    }
//  }
////  class EventTimeChose extends  AssignerWithPeriodicWatermarks[(String, Long)] {
////    var currentMaxTimestamp: Long = 0L
////    val maxOutOfOrderness: Long = 10000 // 10 seconds
////
////    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
////      val timestamp = element._2
////      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
////      timestamp
////    }
////
////    override def getCurrentWatermark: Watermark = {
////      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
////    }
////}
//
//}