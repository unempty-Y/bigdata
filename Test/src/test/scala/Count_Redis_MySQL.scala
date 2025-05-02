import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.sql.{Connection, DriverManager, Statement}
import java.text.SimpleDateFormat

object Count_Redis_MySQL {
  val bigdata1 = "bigdata1"
  val bigdata2 = "bigdata2"
  val bigdata3 = "bigdata3"
  val kafkaPort = 9092
  val redisPort = 6379
  val mysqlPort = 3306
  val mysqlUser_Password = ("root", "123456")
  val clickhousePort = 8123
  val clickhouseUser_Password = ("default", "123456")
  val filter = Array("已发货", "已签收", "已付款") // "已发货", "已付款", "已下单","已签收","已退款"
  private val topic = "ods_mall_data"
  val MysqlData = new OutputTag[Order_master]("MysqlData")
  val totalrefundordercount = new OutputTag[Int]("totalrefundordercount")
  val timeFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  case

  class Ods_mall_data(
                       database: String,
                       table: String,
                       $type: String,
                       ts: Int,
                       xid: Int,
                       commit: Boolean,
                       data: Any
                     )

  case

  class Order_master(
                      order_id: Int,
                      order_sn: String,
                      customer_id: Int,
                      shipping_user: String,
                      province: String,
                      city: String,
                      address: String,
                      order_source: Int,
                      payment_method: Int,
                      order_money: Double,
                      district_money: Double,
                      shipping_money: Double,
                      payment_money: Double,
                      shipping_comp_name: String,
                      shipping_sn: String,
                      create_time: String,
                      shipping_time: String,
                      pay_time: String,
                      receive_time: String,
                      order_status: String,
                      order_point: Int,
                      invoice_title: String,
                      modified_time: String
                    )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(s"$bigdata1:$kafkaPort")
      .setTopics(topic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val orderData = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], topic)
      .map(in => {
        val gson = new Gson
        val ods_mall_data = gson.fromJson(in, classOf[Ods_mall_data])
        ods_mall_data
      })
      .filter(_.table.equals("order_master"))
      .map(in => {
        val gson = new Gson
        val order_master = gson
          .fromJson(gson.toJson(in.data), classOf[Order_master])
        order_master
      })
      // 设置EventTime
      .assignTimestampsAndWatermarks(new choseEventTime)

    val data = orderData
      .keyBy(_ => true)
      .process(new myPF)
    orderData.print()

    val totalRefundordercount = orderData.getSideOutput(totalrefundordercount)

    orderData.getSideOutput(totalrefundordercount)
      .map(("a", _))
      .keyBy(_ => true)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .sum(1)
      .map(_._2.toString)
      .addSink(new redisSink("totalrefundordercount"))

    val toMysql = orderData.getSideOutput(MysqlData)

    toMysql.addSink(new mysqlSink("mysql", "shtd_result", "order_info"))
    env.execute()
  }

  private class choseEventTime extends BoundedOutOfOrdernessTimestampExtractor[Order_master](Time.seconds(5)) {

    override def extractTimestamp(t: Order_master): Long = timeFormat.parse(t.create_time).getTime
  }

  private class myPF extends ProcessFunction[Order_master, String] {
    lazy val PriceCount: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("PriceCount", classOf[Double]))

    override def processElement(i: Order_master, context: ProcessFunction[Order_master, String]#Context, collector: Collector[String]): Unit = {
      if (filter.contains(i.order_status)) {
        val tmp: Double = PriceCount.value() + i.payment_money
        PriceCount.update(tmp)
        collector.collect(PriceCount.value().toString)
      }
      else if (i.order_status.contains("已付款")) {
        context.output(totalrefundordercount, 1)
      }
      else if (i.order_status.contains("已下单")) {
        context.output(MysqlData, i)
      }
    }
  }

  private class redisSink(key: String) extends RichSinkFunction[String] {
    var jedis: Jedis = _

    override def open(parameters: Configuration): Unit = {
      jedis = new Jedis(bigdata1, redisPort)
    }

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      jedis.set(key, value)
    }

    override def close(): Unit = jedis.close()
  }

  private class mysqlSink(dbType: String, dbName: String, tbName: String) extends RichSinkFunction[Order_master] {
    //根据数据库类型建立联系
    private val port = if (dbType.equals("mysql")) mysqlPort else clickhousePort
    private val user = if (dbType.equals("mysql")) mysqlUser_Password._1 else clickhouseUser_Password._1
    private val password = if (dbType.equals("mysql")) mysqlUser_Password._2 else clickhouseUser_Password._2
    private val connectionStr = s"jdbc:$dbType://$bigdata1:$port/$dbName" + (if (dbType.equals("mysql")) "?useSSL=false" else "")

    private var connection: Connection = _
    private var statement: Statement = _

    override def open(parameters: Configuration): Unit = {
      //连接数据库
      connection = DriverManager.getConnection(connectionStr, user, password)
      statement = connection.createStatement()
    }

    override def invoke(value: Order_master, context: SinkFunction.Context): Unit = {
      // 获取类的所有字段（包括继承的字段）
      val fields = value.getClass.getDeclaredFields
      var put = "("
      fields.foreach { field =>
        // 设置字段为可访问，即使它们是私有的
        field.setAccessible(true)
        if (put != "(") put += ","
        // 获取字段的值并根据值的类型进行不同的处理
        field.get(value) match {
          case s: String => put += s"'${s}'"
          case other => put += other
        }
      }
      put += ")"
      val sql = s"insert into $tbName values $put"

      statement.execute(sql)
    }

    override def close(): Unit = {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}