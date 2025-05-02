package gy

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object D_4_1_1 {
  private val bigdata1 = "192.168.45.5"
  private val bootstrapServers = s"${bigdata1}:28749"
  /**
   * @param Id:设备id
   * @param Sum:五分钟生产总数
   */
  case class Totalproduce(Id:String,Sum:Int)

  def main(args: Array[String]): Unit = {
    //设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //设置kafka消费
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",bootstrapServers)
    //抽取kafka数据并处理
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("ProduceRecord", new SimpleStringSchema(), properties))
      //过滤掉未检验的数据
      .filter(data =>{
        val arr = data.split(",")
        arr(9)=="1"
      })
      //数据划分
      .map(data =>{
        val arr = data.split(",")
        Totalproduce(arr(1),arr(8).toInt)
      })
      .keyBy(_.Id)
      //五分钟滚动窗口
      .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
      .sum("Sum")
    //@TODO 输出测试
    dataStream.print()
    //redis输入
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost(bigdata1)
      .setPort(11476)
      .build()
    //读取到redis
    dataStream.addSink(new RedisSink[Totalproduce](conf,new RedisWrite))
    //结束进程
    env.execute()
  }
  private class RedisWrite extends RedisMapper[Totalproduce]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"totalproduce")
    }
    override def getKeyFromData(t: Totalproduce): String = t.Id
    override def getValueFromData(t: Totalproduce): String = t.Sum.toString
  }
}
