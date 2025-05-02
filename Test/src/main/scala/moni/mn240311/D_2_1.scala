package moni.mn240311

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.json4s.JValue
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.logging.SimpleFormatter

object D_2_1 {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  val MaxOutOfOrderness: Long = 5 * 1000L
  private val tuple = ("1001", "1002", "1004")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(s"$BootstrapServers")
      .setTopics("order")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()
    val timeformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line => line.contains("order_info") || line.contains("order_detail"))
      .map(line => {
        val str: JValue = JsonMethods.parse(line, useBigDecimalForDouble = true)
        val JString(create_time): JValue = str \\ "create_time"
        val JString(operate_time): JValue = str \\ "operate_time"
        val JString(order_status): JValue = str \\ "order_status"
        (order_status, if (timeformat.parse(operate_time) == null) timeformat.parse(create_time).getTime else timeformat.parse(operate_time).getTime, timeformat.parse(create_time).getTime)
      })
      .assignTimestampsAndWatermarks(new OrederTime)
      .process(new OrderCount)
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    dataStream.addSink(new RedisSink[(String, Int)](config, new RedisSinkmapper))
    env.execute()
  }

  class OrederTime extends BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.seconds(MaxOutOfOrderness)) {
    override def extractTimestamp(t: (String, Long, Long)): Long = scala.math.max(t._2, t._3)
  }

  class OrderCount extends ProcessFunction[(String, Long, Long), (String, Int)] {
    lazy val Ordercount: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("Ordercount", classOf[Int]))

    override def processElement(i: (String, Long, Long),
                                context: ProcessFunction[(String, Long, Long), (String, Int)]#Context,
                                collector: Collector[(String, Int)]): Unit = {
      if (i._1.contains(tuple)) {
        Ordercount.update(Ordercount.value() + 1)
        collector.collect("totalcount", Ordercount.value())
      }
    }
  }
  class RedisSinkmapper extends RedisMapper[(String, Int)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Int)): String = t._1

    override def getValueFromData(t: (String, Int)): String = t._2.toString
  }
}
