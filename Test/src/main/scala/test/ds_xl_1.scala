//package test
//import org.apache.arrow.flatbuf.Duration
//import org.apache.flink.api.common.eventtime.{TimestampAssigner, WatermarkStrategy}
//import org.apache.flink.api.common.eventtime.WatermarkStrategy
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
//import org.apache.flink.util.Collector
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.streaming.connectors.redis.RedisSink
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
//import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
//import org.apache.flink.util.{Collector, StringUtils}
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//import java.text.SimpleDateFormat
//import java.util.Properties
//import java.util.Properties
//
//
//object ds_xl_1 {
//  import org.apache.flink.api.common.eventtime.{TimestampAssigner, WatermarkStrategy}
//  import org.apache.flink.api.common.serialization.SimpleStringSchema
//  import org.apache.flink.streaming.api.TimeCharacteristic
//  import org.apache.flink.streaming.api.scala._
//  import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
//  import org.apache.flink.streaming.api.windowing.time.Time
//  import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//  val kafkaProps = new Properties()
//  kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
//  kafkaProps.setProperty("group.id", "flink-consumer-group")
//
//  val kafkaConsumer = new FlinkKafkaConsumer[String]("ods_mall_data", new SimpleStringSchema(), kafkaProps)
//  val kafkaStream = env.addSource(kafkaConsumer)
//
//  val orderMasterStream = kafkaStream
//    .filter(_.contains("fact_order_master"))
//    .map(_.split(",")(1)) // Assuming the second field is modified_time
//    .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(2))
//      .withTimestampAssigner(new TimestampAssigner[String] {
//        override def extractTimestamp(element: String, recordTimestamp: Long): Long = element.toLong
//      }))
//    .keyBy(_.toLong)
//    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
//    .process(new OrderMasterProcessFunction())
//
//  // Define the process function for order master stream
//  class OrderMasterProcessFunction extends ProcessWindowFunction[String, String, Long, TimeWindow] {
//    override def process(key: Long, context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
//      val sortedElements = elements.toList.sorted
//      sortedElements.foreach(out.collect)
//    }
//  }
//
//  // Define other streams for different tables and process them accordingly
//
//  orderMasterStream.addSink(new FlinkKafkaProducer[String]("localhost:9092", "fact_order_master", new SimpleStringSchema()))
//
//  // Add sinks for other tables as well
//
//  env.execute("Kafka Flink Data Processing")
//
//}
