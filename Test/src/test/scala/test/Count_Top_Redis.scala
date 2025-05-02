package test

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.json4s.JsonAST.{JDecimal, JInt, JString}
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat
import scala.jdk.CollectionConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

object Count_Top_Redis {

  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  private val topic = "ods_mall_data"
  val MaxOutOfOrderness: Long = 5 * 1000L
  // "已发货", "已付款", "已下单","已签收",
  val tuple = Array("已发货", "已签收")
  val Amount = new OutputTag[(String,String)]("Amount")
  val Consumption = new OutputTag[(String,String)]("Consumption")

  case class tagdata(name: String, data: String)

  case class Order_master_data(
                                order_id: Long,
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(BootstrapServers)
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val orderStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic)
      .filter(_.contains("order_master"))
      .map(line => {
        val str = JsonMethods.parse(line, useBigDecimalForDouble = true)
        val JInt(order_id) = str \\ "order_id"
        val JDecimal(payment_money) = str \\ "payment_money"
        val JString(create_time) = str \\ "create_time"
        val JString(order_status) = str \\ "order_status"
        val JString(modified_time) = str \\ "modified_time"
        (Order_master_data(order_id = order_id.toLong, payment_money = payment_money.toDouble, create_time = create_time.toLong, order_status = order_status, modified_time = timeFormat.parse(modified_time).getTime),
          timeFormat.parse(modified_time).getTime)

      })
      .assignTimestampsAndWatermarks(new choseEventTime)
      .keyBy(_ => true)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .trigger(EventTimeTrigger.create())
      .apply(new putEventData)
.keyBy(_=>true)
      .process(new myPF)
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    orderStream.getSideOutput(Amount).print()
    orderStream.getSideOutput(Consumption).print()
    orderStream.print()
    //    orderStream.getSideOutput(Amount).addSink(new RedisSink[(String, String)](config, new redisSInk))
    //    orderStream.getSideOutput(Consumption).addSink(new RedisSink[(String, String)](config, new redisSInk))
    //    orderStream.addSink(new RedisSink[(String, String)](config, new redisSInk))
    env.execute()
  }

  class myPF extends ProcessFunction[Order_master_data, (String, String)] {
    lazy val totalCount: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("totalCount", classOf[Int]))
    lazy val top3ItemAmount: ListState[(Long, Int)] = getRuntimeContext.getListState(new ListStateDescriptor[(Long, Int)]("top3ItemAmount", classOf[(Long, Int)]))
    lazy val top3ItemConsumption: ListState[(Long, Double)] = getRuntimeContext.getListState(new ListStateDescriptor[(Long, Double)]("top3ItemConsumption", classOf[(Long, Double)]))

    override def processElement(i: Order_master_data, context: ProcessFunction[Order_master_data, (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
      if (tuple.contains(i.order_status)) {
        totalCount.update(totalCount.value() + 1)
        collector.collect("totalcount", totalCount.value().toString)

        top3ItemAmount.add(i.order_id, 1)
        val AmountList = top3ItemAmount.get().asScala.groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
          .sortBy(_._2).reverse
        top3ItemAmount.clear()
        top3ItemAmount.addAll(AmountList.asJava)
        val put1 = AmountList :+ "null" :+ "null"
        context.output(Amount, ("top3itemamount", s"${put1(0)},${put1(1)}${put1(2)}"))
//        collector.collect("top3itemamount", s"${put1(0)},${put1(1)}${put1(2)}")

        top3ItemConsumption.add(i.order_id, i.payment_money)
        val ConsumptionList = top3ItemConsumption.get().asScala.groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
          .sortBy(_._2).reverse
        top3ItemConsumption.clear()
        top3ItemConsumption.addAll(ConsumptionList.asJava)
        val put2 = ConsumptionList :+ "null" :+ "null"
        context.output(Consumption, ("top3itemconsumption", s"${put2(0)}${put2(1)}${put2(2)}"))
//        collector.collect("top3itemconsumption", s"${put2(0)}${put2(1)}${put2(2)}")
      }
    }
  }

  class choseEventTime extends BoundedOutOfOrdernessTimestampExtractor[(Order_master_data,Long)](Time.seconds(5)) {
    override def extractTimestamp(t: (Order_master_data,Long)): Long = t._2
  }

  class putEventData extends WindowFunction[(Order_master_data,Long), Order_master_data, Boolean, TimeWindow] {
    override def apply(key: Boolean, window: TimeWindow, input: Iterable[(Order_master_data,Long)], out: Collector[Order_master_data]): Unit = {
      // 对窗口内的数据按时间戳进行排序
      val sortedInput = input.toList
      // 发出按时间顺序排序后的数据
      sortedInput.foreach { case (line, timestamp) =>
        out.collect(line)
      }
    }
  }

  class redisSInk extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }
}
