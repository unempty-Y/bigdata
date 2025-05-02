package gy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object D_5_3 {

  private val bigdata1 = "192.168.45.5"
  private val BootstrapServers = s"${bigdata1}:28749"

  def main(args: Array[String]): Unit = {
    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度
    env.setParallelism(1)
    // 设置Kafka相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", BootstrapServers)
    // 读取Kafka中ChangeRecord主题的数据
    val datastream = env.addSource(new FlinkKafkaConsumer[String]("ChangeRecord", new SimpleStringSchema(), properties))
      //数据分离
      //只抽取预警数据
      .filter(data =>{
        val arr = data.split(",")
        arr(3)=="预警"
      })
      .map(data => {
        val arr = data.split(",")
        (arr(1),1)
      })

    // 计算最近3分钟预警次数最多的设备
    val warnings = datastream
      //根据设备id切分数据
      .keyBy(_._1)
      //隔1分钟输出最近3分钟预警次数总数 ---滑动窗口
      .window(SlidingProcessingTimeWindows.of(Time.minutes(3),Time.minutes(1)))
      //设备预警次数求和
      .sum(1)
      //根据设备id进行分割
      .keyBy(_._1)
      //取出预警次数最大的设备id
      .maxBy(1)

      //@TODO 测试数据
    warnings.print()
    // 将结果存入Redis中
    val redisConfig = new FlinkJedisPoolConfig.Builder()
      .setHost(bigdata1)
      .setPort(11476)
      .build()
    //写入redis
    //@TODO 窗口输出:结束时间,预警次数最大的设备id
    warnings.addSink(new RedisSink[(String, Int)](redisConfig,new WarningRedis))
    env.execute()
  }

  //设置连接redis类 
  private class WarningRedis extends RedisMapper[(String,Int)]{
    //设置哈希表类型
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"warning_last3min_everymin_out")
    }
    //设置键为数据产生时间（不确定是否为窗口结束时间）
    override def getKeyFromData(data: (String,Int)): String = {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
      format
    }
    //设置值为id
    override def getValueFromData(data: (String,Int)): String = {
      data._1
    }
  }
}