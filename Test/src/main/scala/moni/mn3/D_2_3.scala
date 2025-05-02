package moni.mn3

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
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
import org.apache.hadoop.conf
import org.json4s.JValue
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import java.text.SimpleDateFormat
import java.util.Date

object D_2_3 {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  val MaxOutOfOrderness: Long = 5 * 1000L
  // "已发货", "已付款", "已下单","已签收",
  val tuple = Array("已发货", "已签收")
  val refundOrder: OutputTag[Order_master_data] = new OutputTag[Order_master_data]("refund") {}

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(40000)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1))
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
        Order_master_data(order_id.toLong, payment_money.toDouble, create_time.toLong, order_status, if (modified_time == null) create_time.toLong else timeformat.parse(modified_time).getTime)
      })
      //      .assignTimestampsAndWatermarks(new OrederTime)
      .keyBy(_ => true)
      .process(new top2User)

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    val totalrefundordercount = dataStream.getSideOutput[Order_master_data](refundOrder)
    totalrefundordercount.addSink(new HbaseSink("192.168.45.16", "test", "order_detail", "info"))
    totalrefundordercount.print()
    dataStream
      .map(a => ("totalrefundordercount", s"[${a(0)},${a(1)}]"))
      .filter(_._1 == "totalprice")
      .addSink(new RedisSink[(String, String)](config, new RedisSinkmapper))
    env.execute("flink1")
  }

  class OrederTime extends BoundedOutOfOrdernessTimestampExtractor[Order_master_data](Time.seconds(MaxOutOfOrderness)) {
    override def extractTimestamp(t: Order_master_data): Long = scala.math.max(t.create_time, t.modified_time)
  }

  class top2User extends ProcessFunction[Order_master_data, List[String]] {
    lazy val valueStateList: ListState[(Long, Double)] = getRuntimeContext.getListState(new ListStateDescriptor[(Long, Double)]("valueStateList", classOf[(Long, Double)]))

    override def processElement(i: Order_master_data, context: ProcessFunction[Order_master_data, List[String]]#Context, collector: Collector[List[String]]): Unit = {
      if (tuple.contains(i.order_status)) {
        valueStateList.add((i.order_id, i.payment_money))
        import scala.collection.JavaConverters._
        val valueList = valueStateList.get()
          .asScala
          .toList
          //根据商品Id分组累加
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
          //根据商品销售总额排序
          .sortBy(_._2).reverse
        valueStateList.clear()
        valueStateList.addAll(valueList.asJava)
        val output = valueList.map(line => s"${line._1}:${line._2}") :+ "null" :+ "null"
        collector.collect(output)
      }
      else if (i.order_status.contains("已下单")) {
        context.output(refundOrder, (i))

      }
    }
  }

  class RedisSinkmapper extends RedisMapper[(String, String)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }

  import org.apache.flink.configuration.Configuration
  import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
  import org.apache.hadoop.hbase._
  import org.apache.hadoop.hbase.client._


  import org.apache.flink.api.common.io.OutputFormat
  import org.apache.flink.api.java.tuple.Tuple2
  import org.apache.hadoop.hbase.client.{Put, Connection, ConnectionFactory, Table}
  import org.apache.hadoop.hbase.util.Bytes

  class HBaseMultipleOutputFormat extends OutputFormat[Tuple2[String, String]] {

    @transient private var connection: Connection = _
    @transient private var table1: Table = _
    @transient private var table2: Table = _

    override def configure(parameters: Configuration): Unit = {
      // 初始化HBase连接
      connection = ConnectionFactory.createConnection()
      table1 = connection.getTable(TableName.valueOf("table1"))
      table2 = connection.getTable(TableName.valueOf("table2"))
    }

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Tuple2[String, String]): Unit = {
      val key = record.f0
      val value = record.f1

      val put1 = new Put(Bytes.toBytes(key))
      put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(value))
      table1.put(put1)

      val put2 = new Put(Bytes.toBytes(key))
      put2.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("col2"), Bytes.toBytes(value))
      table2.put(put2)
    }

    override def close(): Unit = {
      // 关闭HBase连接
      table1.close()
      table2.close()
      connection.close()
    }
  }
  private class HbaseSink(zookeeper: String, namespace: String, table: String, family: String) extends RichSinkFunction[Order_master_data] {
    private var connection: Connection = _
    private var htable: Table = _

    override def open(parameters: Configuration): Unit = {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      connection = ConnectionFactory.createConnection(hbaseConf)
      htable = connection.getTable(TableName.valueOf(namespace, table))
    }

    override def close(): Unit = {
      if (connection != null) {
        connection.close()
      }
      if (htable != null) {
        htable.close()
      }
    }

    override def invoke(value: Order_master_data, context: SinkFunction.Context): Unit = {
      //rowkey:设备id-系统时间
      val id = value.order_id
      val rowkey = s"${id}"

      val put = new Put(rowkey.getBytes())
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("order_id"), Bytes.toBytes(value.order_id.toString))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("order_status"), Bytes.toBytes(value.order_status))
      htable.put(put)
    }
  }
}
