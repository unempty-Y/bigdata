package ds

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.{Collector, StringUtils}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Properties
object ds_3 {

  // 输入事件类型
  case class OrderEvent(id: Long,
                        consignee: String,
                        consignee_tel: String,
                        final_total_amount: Double,
                        order_status: String,
                        user_id: Long,
                        delivery_address: String,
                        order_comment: String,
                        out_trade_no: String,
                        trade_body: String,
                        create_time: Long,
                        operate_time: Long,
                        province_id: Int,
                        feight_fee: Double)

  private val bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"${bigdata1}:9092"

  // watrmark 允许数据延迟时间
  val MaxOutOfOrderness: Long = 5 * 1000L
  // 有效订单
  val avaliableOrderStatus = Array("1001","1002","1004")
  // 定义一个侧输出的标签，标识“用户退款消费额”侧输出流
  val undoOrder:OutputTag[OrderEvent] = new OutputTag[OrderEvent]("undo") {}
  def main(args: Array[String]) {
    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度
    env.setParallelism(1)
    //设置kafka消费者约束
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",BootstrapServers)

    // 有效订单
//    val avaliableOrderStatus = Array("1001","1002","1004")
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val orderStream: DataStream[(String,Double)]  = env.addSource(new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties)) // 指定Kafka数据源

      .map(line => {  // 转换为OrderEvent对象
        val arr = line.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(2),
          arr(3).toDouble,arr(4),arr(5).toLong,
          arr(6),arr(7),arr(8),arr(9),
          fm.parse(arr(10)).getTime,      // create_time
          if(StringUtils.isNullOrWhitespaceOnly(arr(11))) fm.parse(arr(10)).getTime else fm.parse(arr(11)).getTime,      // operate_time
          arr(12).toInt,arr(13).toDouble)
      })
      .assignTimestampsAndWatermarks(new OrderTSExtractor)
      .map((1, _))
      .keyBy(_._1)
      .process(new MyOrderProcessFunction)
    orderStream.print()
    // 将主流数据发往redis sink
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(bigdata1)
      .setPort(11476)
      .build()
    orderStream.addSink(new RedisSink[(String,Double)](config, new RedisSinkMapper))

    // 侧输出流处理：写入mysql
    val undoOrderStream = orderStream.getSideOutput[OrderEvent](undoOrder)
    //  Flink-1.10及以前，没有提供JDBC Sink, 使用自定义的Sink
    undoOrderStream.addSink(new MyJDBCSink)

    // execute program
    env.execute("Flink Streaming Task02")
  }

  /**
   * 为订单记录分配时间戳
   * 水印（Watermarks）是位于最大时间戳后的一个固定时间周期，定期生成。
   */
  class OrderTSExtractor extends BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(MaxOutOfOrderness)) {
    // 抽取时间戳
    def extractTimestamp(order: OrderEvent): Long = scala.math.max(order.operate_time, order.create_time)
  }

  // 自定义ProcessFunction函数
  class MyOrderProcessFunction extends ProcessFunction[(Int,OrderEvent), (String,Double)] {

    // 由此函数所维护的存储状态
    // 累加最终成交金额
    private var lastOrderAmount: ValueState[Double] = _

    // 初始化
    override def open(parameters: Configuration): Unit = {
      lastOrderAmount = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastAmount", classOf[Double]))
    }

    // 处理每一个元素，累加总金额
    override def processElement(input: (Int,OrderEvent),
                                context: ProcessFunction[(Int,OrderEvent), (String,Double)]#Context,
                                out: Collector[(String,Double)]): Unit = {
      // 获得当前订单的状态
      val currentOrderStatus = input._2.order_status

      // 如果是有效的订单("1001","1002","1004")
      if(avaliableOrderStatus.contains(currentOrderStatus)){
        // 将当前订单记录的金额累加到上一次的总金额上
        var accAmount = lastOrderAmount.value() + input._2.final_total_amount

        // 将当前总金额发送到下游
        out.collect(("totalprice",accAmount))

        // 并更新状态
        lastOrderAmount.update(accAmount)
      }
      // 如果是“取消订单”状态的订单
      else if("1003" == currentOrderStatus){
        // 将当前被取消的订单发送到侧输出流"undoOrder"
        context.output(undoOrder, input._2)
      }
    }
  }

  // redisMap接口，设置key和value
  // Redis Sink 核心类是 RedisMappe 接口，使用时我们要编写自己的redis操作类实现这个接口中的三个方法
  class RedisSinkMapper extends RedisMapper[(String,Double)] {
    // getCommandDescription：设置数据使用的数据结构
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.SET)      // 指定存储类型
    }
    override def getKeyFromData(event: (String,Double)): String = event._1
    // 指定value
    override def getValueFromData(event: (String,Double)): String = event._2.toString
  }

  // 指定Sink的范型People
 private class MyJDBCSink() extends RichSinkFunction[OrderEvent] {
    private var conn: Connection = _
    private var stmt: PreparedStatement = _
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 定义mysql数据库连接url和驱动程序及账号、密码
      val url = "jdbc:mysql://192.168.45.16:3306/shtd_industry?useSSL=false"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "123456"
      //定义SQL的连接、预编译器,给定初始值占位符
      Class.forName(driver) // 加载驱动程序
      conn = DriverManager.getConnection(url, username, password) // 连接数据库
      // 执行SQL语句
      val sql =
        """
          |insert into order_info (id, consignee, consignee_tel, final_total_amount,
          |       order_status, user_id, delivery_address, order_comment, out_trade_no,
          |       trade_body, create_time, operate_time, province_id, feight_fee)
          |values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |""".stripMargin
      stmt = conn.prepareStatement(sql)
    }

    /**
     * 调用方法,调用逻辑都是在里面，数据类型、格式组合都是在里面进行
     * 编写
     *
     * @param value
     */
    override def invoke(value: OrderEvent): Unit = {
      // 传入参数
      stmt.setLong(1, value.id)
      stmt.setString(2, value.consignee)
      stmt.setString(3, value.consignee_tel)
      stmt.setDouble(4, value.final_total_amount)
      stmt.setString(5, value.order_status)
      stmt.setLong(6, value.user_id)
      stmt.setString(7, value.delivery_address)
      stmt.setString(8, value.order_comment)
      stmt.setString(9, value.out_trade_no)
      stmt.setString(10, value.trade_body)
      stmt.setLong(11, value.create_time)
      stmt.setLong(12, value.operate_time)
      stmt.setInt(13, value.province_id)
      stmt.setDouble(14, value.feight_fee)
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

