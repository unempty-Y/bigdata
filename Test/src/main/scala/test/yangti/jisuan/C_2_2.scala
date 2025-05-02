//package test.yangti.jisuan
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.streaming.connectors.redis.RedisSink
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
//import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
//import org.apache.flink.util.{Collector, StringUtils}
//import org.json4s.JsonAST.{JDecimal, JInt, JString}
//import org.json4s.jackson.JsonMethods
//
//import java.text.SimpleDateFormat
//import java.util.Properties
//
//object C_2_2 {
//
//  // 输入事件类型
//  case class OrderEvent(order_detail_id:BigInt,
//    fee_money:Double,
//                        create_time:Long
//  )
//
//  private val bigdata1 = "192.168.45.16"
//  private val BootstrapServers = s"${bigdata1}:9092"
//  // watrmark 允许数据延迟时间
//  val MaxOutOfOrderness: Long = 5 * 1000L
//
//  def main(args: Array[String]) {
//    // 设置流执行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 设置事件时间
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    // 设置并行度
//    env.setParallelism(1)
//    //设置kafka消费者约束
//    val value: KafkaSourceBuilder[Nothing] = KafkaSource.builder()
//    value
//      .map((1, _))
//      .keyBy(_._1)
//      .process(new GMVcount)
//    //测试
//    orderStream.print()
//    //写入redis
//    val config = new FlinkJedisPoolConfig.Builder()
//      .setHost(bigdata1)
//      .setPort(6379)
//      .build()
//    orderStream.addSink(new RedisSink[(Long, Double)](config, new RedisSinkMapper))
//    // execute program
//    env.execute("Flink Streaming Task01")
//  }
//
//  /**
//   * 为订单记录分配时间戳
//   * 水印（Watermarks）是位于最大时间戳后的一个固定时间周期，定期生成。
//   */
//
//  /**
//   * 自定义ProcessFunction函数
//   */
//  private class GMVcount extends ProcessFunction[(Int, OrderEvent), (Long, Double)] {
//    lazy val stateAndTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("velue-Time", classOf[Long]))
//    lazy val IDandGMV: ValueState[(Long, Double)] = getRuntimeContext.getState(new ValueStateDescriptor[(Long, Double)]("IDandGMV", classOf[(Long, Double)]))
//
//    override def processElement(data: (Int, OrderEvent), context: ProcessFunction[(Int, OrderEvent), (Long, Double)]#Context, collector: Collector[(Long, Double)]): Unit = {
//
//      val newstateAndTime = stateAndTime.value()
//      //获取当前时间
//      val newTime: Long = data._2.create_time
//      //获取本次金额
//      val newAmount = data._2.fee_money
//
//      if (newstateAndTime != null) {
//        //获取上次时间
//        val lastTime = newstateAndTime
//        //判断获取1分钟内GMV
//        if (newTime - lastTime >= 600) {
//          stateAndTime.update(newTime)
//          collector.collect(IDandGMV.value())
//          IDandGMV.clear()
//        }
//        else {
//          val GMV: Double = IDandGMV.value()._2 + newAmount
//          IDandGMV.update(data._2.order_detail_id.toLong, GMV)
//        }
//      }
//
//    }
//  }
//
//  /**
//   * redisMap接口，设置key和value
//   * Redis Sink 核心类是 RedisMappe 接口，使用时我们要编写自己的redis操作类实现这个接口中的三个方法
//   */
//
//  class RedisSinkMapper extends RedisMapper[(Long, Double)] {
//    // getCommandDescription：设置数据使用的数据结构
//    override def getCommandDescription: RedisCommandDescription = {
//      new RedisCommandDescription(RedisCommand.SET) // 指定存储类型
//    }
//
//    override def getKeyFromData(event: (Long, Double)): String = "store_gmv"
//    override def getValueFromData(event: (Long, Double)): String = event._2.toString
//  }
//}
//
//
