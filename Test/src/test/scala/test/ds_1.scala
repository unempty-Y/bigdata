package test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.{Collector, StringUtils}

import java.text.SimpleDateFormat
import java.util.Properties

object ds_1 {

  // 输入事件类型
  case class OrderEvent(id:Long,
                        consignee:String,
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

  private val bigdata1 = "192.168.45.21"
  private val BootstrapServers = s"${bigdata1}:9092"
  // watrmark 允许数据延迟时间
  val MaxOutOfOrderness: Long = 5 * 1000L
  def main(args: Array[String]) {
    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度
    env.setParallelism(1)
    //设置kafka消费者约束
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", BootstrapServers)
    val orderStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("order2", new SimpleStringSchema(), properties))
    // 有效订单
    //    val avaliableOrderStatus = Array("1001","1002","1004")
    //    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    val orderStream: DataStream[(String,Double)]  = env.addSource(new FlinkKafkaConsumer[String]("order", new SimpleStringSchema(), properties)) // 指定Kafka数据源
    // 分割字段
    //      .map(line => line.split(","))
    // 注意，有可能有不完整数据(比如，缺少字段)
    //      .filter(arr => arr.length==14)
    // 转换为OrderEvent对象
    //      .map(arr => {
    //        OrderEvent(arr(0).toLong,arr(1),arr(2),
    //          arr(3).toDouble,arr(4),arr(5).toLong,
    //          arr(6),arr(7),arr(8),arr(9),
    //          fm.parse(arr(10)).getTime,      // create_time
    //          if(StringUtils.isNullOrWhitespaceOnly(arr(11))) fm.parse(arr(10)).getTime else fm.parse(arr(11)).getTime,      // operate_time
    //          arr(12).toInt,arr(13).toDouble)
    //      })
    //      .assignTimestampsAndWatermarks(new OrderTSExtractor)
    //
    //      .filter(order => avaliableOrderStatus.contains(order.order_status))   // 过滤有效订单("1001","1002","1004")
    //      .map((1, _))
    //      .keyBy(_._1)
    //      .process(new MyOrderProcessFunction)
    //测试
    orderStream.print()
    env.execute()
  }}
    //写入redis
//    val config = new FlinkJedisPoolConfig.Builder()
//      .setHost(bigdata1)
//      .setPort(6379)
//      .build()
//    orderStream.addSink(new RedisSink[(String,Double)](config, new RedisSinkMapper))
//    // execute program
//    env.execute("Flink Streaming Task01")
//  }

  /**
   * 为订单记录分配时间戳
   * 水印（Watermarks）是位于最大时间戳后的一个固定时间周期，定期生成。
   */
//  class OrderTSExtractor extends BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(MaxOutOfOrderness)) {
//    // 抽取时间戳
//    def extractTimestamp(order: OrderEvent): Long = scala.math.max(order.operate_time, order.create_time)
//  }
//
//  /**
//   * 自定义ProcessFunction函数
//   */
//  class MyOrderProcessFunction extends KeyedProcessFunction[Int, (Int,OrderEvent), (String,Double)] {
//
//    // 由此函数所维护的存储状态：累加最终金额
//    private var lastOrderAmount: ValueState[Double] = _
//
//    // 初始化
//    override def open(parameters: Configuration): Unit = {
//      lastOrderAmount = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastAmount", classOf[Double]))
//    }
//
//    // 处理每一个元素，累加总金额
//    override def processElement(input: (Int,OrderEvent),
//                                context: KeyedProcessFunction[Int, (Int,OrderEvent), (String,Double)]#Context,
//                                out: Collector[(String,Double)]): Unit = {
//
//      // 将当前订单记录的金额累加到上一次的总金额上
//      var accAmount = lastOrderAmount.value() + input._2.final_total_amount
//
//      // 将当前总金额发送到下游
//      out.collect(("totalprice",accAmount))
//
//      // 将当前订单记录的金额累加到上一次的总金额上,并更新状态
//      lastOrderAmount.update(accAmount)
//    }
//  }
//
//  /**
//   * redisMap接口，设置key和value
//   * Redis Sink 核心类是 RedisMappe 接口，使用时我们要编写自己的redis操作类实现这个接口中的三个方法
//   */
//
//  class RedisSinkMapper extends RedisMapper[(String,Double)] {
//    // getCommandDescription：设置数据使用的数据结构
//    override def getCommandDescription: RedisCommandDescription = {
//      new RedisCommandDescription(RedisCommand.SET)     // 指定存储类型
//    }
//    override def getKeyFromData(event: (String,Double)): String = event._1
//    override def getValueFromData(event: (String,Double)): String = event._2.toString
//  }
//}


