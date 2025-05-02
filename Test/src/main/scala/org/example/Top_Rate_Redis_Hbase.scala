package org.example

import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._

import scala.jdk.CollectionConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
import scala.util.Random

object Top_Rate_Redis_Hbase {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  private val topic = "ods_mall_data"
  val MaxOutOfOrderness: Long = 5 * 1000L
  // "已发货", "已付款", "已下单","已签收",
  val tuple = Array("已发货", "已签收")
  val canceltag = new OutputTag[Order_master]("canceltag")

  def main(args: Array[String]): Unit = {
    val handler = FlinkHandlerHelper.flinkHandlerHelper()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(BootstrapServers)
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val orderData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic)
      .map(in => {
        val gson = new Gson
        val ods_mall_data: Ods_mall_data = gson.fromJson(in, classOf[Ods_mall_data])
        ods_mall_data
      })
      .filter(_.table.contains("order_master"))
      .map(in => {
        val gson = new Gson
        val order_master: Order_master = gson.fromJson(gson.toJson(in.data), classOf[Order_master])
        order_master
      })
    orderData.print()
    //      // 设置EventTime
    //      .assignTimestampsAndWatermarks(new choseEventTime)
    //      .keyBy(_ => true)
    //      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    //      .trigger(EventTimeTrigger.create())
    //      .apply(new putEventData)

    //    val dwdData = orderData.keyBy(_ => true)
    //      .process(new myPF)
    val cancel = orderData

//      .map(i => {
//                val num: Int = Random.nextInt(10)
//                val name = new getAttributeName
//                val listName: Array[String] = name.getClassFields(classOf[Order_master])
//                val value: Array[String] = i.productIterator.map(_.toString).toArray
//                val tuples: Array[(String, String, String)] = listName.zip(value).map { case (l, v) => ("info", l, v) }
//                (s"$num${i.create_time}", tuples)
//              })
//      .addSink(handler.hbaseWrite("test:qwe"))
    //    val cancel = dwdData
    //      .getSideOutput(canceltag)
    //    val cancelrate = cancel
    //      .map(_ => ("cancelrate", 1.0))
    //      .union(orderData.map(_ => ("all", 1.0)))
    //      .keyBy(_ => true)
    //      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    //      .apply(new winFunction)

    //    val config = new FlinkJedisPoolConfig.Builder()
    //      .setHost(Bigdata1)
    //      .setPort(6379)
    //      .build()

    //1      要在Redis get到中文 redis-cli --raw
    //    dwdData.addSink(new RedisSink[(String, String)](config, new redisSink))

    //2
    //    cancelrate.addSink(new RedisSink[(String, String)](config, new redisSink))
    //3     在hbase显示中文 scan 'test:order_master', {FORMATTER => 'toString',COLUMNS => 'info:shipping_user',LIMIT => 5,REVERSED => true}
//    cancel.addSink(new writeHbase(Bigdata1, "test", "order_master", "info"))
    env.execute()
  }

  class putEventData extends WindowFunction[Order_master, Order_master, Boolean, TimeWindow] {
    override def apply(key: Boolean, window: TimeWindow, input: Iterable[Order_master], out: Collector[Order_master]): Unit = {
      // 对窗口内的数据按时间戳进行排序
      val sortedInput = input.toList
      // 发出按时间顺序排序后的数据
      sortedInput.foreach { case (in) =>
        out.collect(in)
      }
    }
  }

  class winFunction extends WindowFunction[(String, Double), (String, String), Boolean, TimeWindow] {
    override def apply(key: Boolean, window: TimeWindow, input: Iterable[(String, Double)], out: Collector[(String, String)]): Unit = {
      val str = input.toList.groupBy(_._1).mapValues(_.map(_._2).sum)
      val cancel = str.getOrElse("cancelrate", 0.0)
      val all = str.getOrElse("all", 0.0)
      val value = ((cancel / all) * 1000).round / 10.0
      out.collect("cancelrate", s"$value%")
    }
  }

  class myPF extends ProcessFunction[Order_master, (String, String)] {
    lazy val top2UserConsumption: ListState[((Long, String), Double)] = getRuntimeContext.getListState(new ListStateDescriptor[((Long, String), Double)]("top2UserConsumption", classOf[((Long, String), Double)]))

    override def processElement(i: Order_master, context: ProcessFunction[Order_master, (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
      if (tuple.contains(i.order_status)) {
        top2UserConsumption.add((i.order_id, i.shipping_user), i.payment_money)
        val longToTuples = top2UserConsumption.get().asScala.groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList.sortBy(_._2)
          .reverse
        top2UserConsumption.clear()
        top2UserConsumption.addAll(longToTuples.asJava)
        val put = longToTuples.map(d => s"${d._1._1}:${d._1._2}:${d._2}") :+ "null"
        collector.collect("top2userconsumption", s"[${put(0)},${put(1)}]")

      }
      else if (i.order_status.contains("已付款"))
        context.output(canceltag, i)
    }
  }

  class redisSink extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }

  private class writeHbase(zookeeper: String, namepace: String, table: String, family: String) extends RichSinkFunction[Order_master] {
    private var connection: Connection = _
    private var htable: Table = _

    override def open(parameters: Configuration): Unit = {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      connection = ConnectionFactory.createConnection(hbaseConf)
      htable = connection.getTable(TableName.valueOf(namepace, table))
    }

    override def close(): Unit = {
      if (connection != null) {
        connection.close()
      }
      if (htable != null) {
        htable.close()
      }
    }

    override def invoke(value: Order_master, context: SinkFunction.Context): Unit = {
      val num: Int = Random.nextInt(10)
      val name = new getAttributeName
      val listName: Array[String] = name.getClassFields(classOf[Order_master])
      val values: Array[String] = value.productIterator.map(_.toString).toArray
      val tuples: Array[(String, String, String)] = listName.zip(values).map { case (l, v) => ("info", l, v) }
      val rowkey = s"$num${value.create_time}"
      val put = new Put(rowkey.getBytes())
      for (i <- tuples) {
        put.addColumn(i._1.getBytes, i._2.getBytes, i._3.getBytes)
      }
      htable.put(put)
    }
  }
}