package moni.mn240311_2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat

object D_2_2 {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  val MaxOutOfOrderness: Long = 5 * 1000L
  // "已发货", "已付款", "已下单","已签收",
  val tuple = Array("已发货", "已签收")

  case class Order_master_data(
                                order_id: BigInt,
                                //                                order_sn: Long,
                                //                                customer_id: Long,
                                //                                shipping_user: String,
                                //                                province: String,
                                //                                city: String,
                                //                                address: String,
                                //                                order_source: String,
                                //                                payment_method: Int,
                                //                                order_money: Double,
                                //                                district_money: Double,
                                //                                shipping_money: Double,
                                payment_money: Double,
                                //                                shipping_comp_name: String,
                                //                                shipping_sn: Long,
                                create_time: Long,
                                //                                shipping_time: Long,
                                //                                pay_time: Long,
                                //                                receive_time: Long,
                                order_status: String,
                                //                                order_point: Int,
                                //                                invoice_title: String,
                                modified_time: Long
                              )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(40000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,1))
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(BootstrapServers)
      .setTopics("ods_mall_data")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val timeformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line => line.contains("order_master"))
      .map(line => {
        val str: JValue = JsonMethods.parse(line, useBigDecimalForDouble = true)
        val JInt(order_id): JValue = str \\ "order_id"
        val JDecimal(payment_money): JValue = str \\ "payment_money"
        val JString(create_time): JValue = str \\ "create_time"
        val JString(order_status): JValue = str \\ "order_status"
        val JString(modified_time): JValue = str \\ "modified_time"
        Order_master_data(order_id, payment_money.toDouble, create_time.toLong, order_status, if (modified_time == null) create_time.toLong else timeformat.parse(modified_time).getTime)
      })
      //      .assignTimestampsAndWatermarks(new OrederTime)
      .keyBy(_ => true)
      .process(new PaymentCountPF)
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    val totalrefundordercount = dataStream.filter(_._1 == "totalrefundordercount")
    totalrefundordercount.addSink(new RedisSink[(String, Double)](config, new RedisSinkmapper))
    totalrefundordercount.print()
    dataStream.filter(_._1 == "totalprice").addSink(new RedisSink[(String, Double)](config, new RedisSinkmapper))
    env.execute("flink1")
  }

  class OrederTime extends BoundedOutOfOrdernessTimestampExtractor[Order_master_data](Time.seconds(MaxOutOfOrderness)) {
    override def extractTimestamp(t: Order_master_data): Long = scala.math.max(t.create_time, t.modified_time)
  }

  class PaymentCountPF extends ProcessFunction[Order_master_data, (String, Double)] {
    lazy val PaymentCount: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("Ordercount", classOf[Double]))

    override def processElement(i: Order_master_data,
                                context: ProcessFunction[Order_master_data, (String, Double)]#Context,
                                collector: Collector[(String, Double)]): Unit = {
      if (tuple.contains(i.order_status)) {
        PaymentCount.update(PaymentCount.value() + i.payment_money)
        collector.collect("totalprice", PaymentCount.value())
      }
      else if (i.order_status.contains("已下单")) collector.collect("totalrefundordercount", i.payment_money)
    }
  }

  class RedisSinkmapper extends RedisMapper[(String, Double)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Double)): String = t._1

    override def getValueFromData(t: (String, Double)): String = t._2.toString
  }
}
