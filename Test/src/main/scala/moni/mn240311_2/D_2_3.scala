package moni.mn240311_2

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
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

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

object D_2_3 {
  private val Bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"$Bigdata1:9092"
  val MaxOutOfOrderness: Long = 5 * 1000L
  // "已发货", "已付款", "已下单","已签收",
  val tuple = Array("已发货", "已签收")

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
  val undoOrder:OutputTag[Order_master_data] = new OutputTag[Order_master_data]("undo") {}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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
      .process(new PaymentCountPF)
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(Bigdata1)
      .setPort(6379)
      .build()
    val totalrefundordercount = dataStream.filter(_._1 == "totalrefundordercount")
    dataStream.filter(_._1 == "totalprice").addSink(new RedisSink[(String, Double)](config, new RedisSinkmapper))

    val undoOrderStream = dataStream.getSideOutput[Order_master_data](undoOrder)
    //  Flink-1.10及以前，没有提供JDBC Sink, 使用自定义的Sink
    undoOrderStream.addSink(new MyJDBCSink)
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
      else if (i.order_status.contains("已付款"))
        context.output(undoOrder, i)
    }
  }

  class RedisSinkmapper extends RedisMapper[(String, Double)] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Double)): String = t._1

    override def getValueFromData(t: (String, Double)): String = t._2.toString
  }

  private class MyJDBCSink() extends RichSinkFunction[Order_master_data] {
    private var conn: Connection = _
    private var stmt: PreparedStatement = _
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 定义mysql数据库连接url和驱动程序及账号、密码
      val url = "jdbc:mysql://192.168.45.16:3306/shtd_result?useSSL=false"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "123456"
      //定义SQL的连接、预编译器,给定初始值占位符
      Class.forName(driver) // 加载驱动程序
      conn = DriverManager.getConnection(url, username, password) // 连接数据库
      // 执行SQL语句
      val sql =
        """
          |insert into order_info (order_id, payment_money, create_time, order_status,
          |       modified_time)
          |values (?, ?, ?, ?, ?)
          |""".stripMargin
      stmt = conn.prepareStatement(sql)
    }

    /**
     * 调用方法,调用逻辑都是在里面，数据类型、格式组合都是在里面进行
     * 编写
     */
    override def invoke(value: Order_master_data): Unit = {
      // 传入参数
      stmt.setLong(1, value.order_id)
      stmt.setDouble(2, value.payment_money)
      stmt.setLong(3, value.create_time)
      stmt.setString(4, value.order_status)
      stmt.setLong(5, value.modified_time)
      // 执行update SQL
      stmt.executeUpdate
    }

    /**
     * close方法用来关闭资源，清理工作，类似于Finally
     */
    override def close(): Unit = {
      super.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }
}
