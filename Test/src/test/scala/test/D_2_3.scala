package test

import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.json4s.JValue
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat

object D_2_3 {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  val MaxOutOfOrderness: Long = 5 * 1000L
  private val tuple = ("1001", "1002", "1004")
  val cancelratetag: OutputTag[(String, String)] = new OutputTag[(String, String)]("cancelrate") {}

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

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    dataStream
      .process(new OrderCount)
      .addSink(new RedisSink[(String, String)](config, new RedisSinkmapper))

    val cancelrate = dataStream.getSideOutput[(String, String)](cancelratetag).union(dataStream.map(_ => ("allcount", "1")))
      .keyBy(_=>true)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply((key: Boolean, window: TimeWindow, input: Iterable[(String, String)], out: Collector[(String, String)]) => {
        val stringToTuples = input.toList.groupBy(_._1).mapValues(_.map(_ => 1.0).sum)
        val allcount = stringToTuples.getOrElse("allcount", 0.0)
        val cancelcount = stringToTuples.getOrElse("cancelrate", 0.0)
        val value = ((cancelcount / allcount) * 1000).round/10.0
        out.collect("cancelrate", s"$value%")
      })

    cancelrate.addSink(new RedisSink[(String, String)](config, new RedisSinkmapper))
env.execute()
  }

  class OrederTime extends BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.seconds(MaxOutOfOrderness)) {
    override def extractTimestamp(t: (String, Long, Long)): Long = scala.math.max(t._2, t._3)
  }

  class OrderCount extends ProcessFunction[(String, Long, Long), (String, String)] {
    lazy val Ordercount: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("Ordercount", classOf[Double]))

    override def processElement(i: (String, Long, Long),
                                context: ProcessFunction[(String, Long, Long), (String, String)]#Context,
                                collector: Collector[(String, String)]): Unit = {

      if (i._1.contains(tuple)) {
        Ordercount.update(Ordercount.value() + 1)
        collector.collect("totalcount", Ordercount.value().toString)
      }
      else if (i._1.contains("1003"))
        context.output(cancelratetag, ("cancelrate", "1"))
    }
  }

  class RedisSinkmapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }
}
