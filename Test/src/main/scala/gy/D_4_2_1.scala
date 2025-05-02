package gy

import java.text.SimpleDateFormat
import java.util.Properties

import javax.script.SimpleScriptContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

object D_4_2_1 {
  private val bigdata1 = "192.168.45.5"
  private val BootstrapServers = s"${bigdata1}:28749"

  def main(args: Array[String]): Unit = {
    //执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行量
    env.setParallelism(1)
    //kafka消费者设置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",BootstrapServers)
    //kafka写入
    val datastream = env.addSource(new FlinkKafkaConsumer("ChangeRecord", new SimpleStringSchema(), properties))
      .map(data => {
        val arr = data.split(",")
        //获取时间戳
        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(4)).getTime
        (arr(1),arr(3),time)
      })
        .assignAscendingTimestamps(_._3)
        .keyBy(_._1)
        .process(new WarningFunction)
    //测试数据
    datastream.print()
    //写入redis
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(bigdata1)
      .setPort(11476)
      .build()
    datastream.addSink(new RedisSink[(String, String)](config,new WriteRedis))
    //结束测试
    env.execute()
  }

  /**
   * String: 设备id
   * (String,String,Long): 设备id，设备状态，开始时间
   * (String,String):输出数据
   */
  //设置类
  private class  WarningFunction extends KeyedProcessFunction[String,(String,String,Long),(String,String)]{
    //初始化存入(状态,时间)
    lazy val state = getRuntimeContext.getState(new ValueStateDescriptor[(String,Long)]("warning-state",classOf[(String,Long)]))
    override def processElement(value: (String, String, Long), context: KeyedProcessFunction[String, (String, String, Long), (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
      //读取数据(状态,时间)
      val currState = state.value()
      //获取当前预警时间
      val warningTime = value._3
      //判断状态是否为预警状态
      if (value._2 == "预警"){
        //判断是否为空
        if(currState == null ){
          state.update((value._2,warningTime))
        }else{
          //时间差
          //获取第一次预警时间
          val currTime = currState._2
          val DTime = warningTime - currTime
          //判断时间是否连续30秒预警
          if (DTime >= 30000){
            collector.collect(value._1,s"${value._1},${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value._3)}:设备${value._1} 连续30秒为预警状态请尽快处理！")
            state.update((value._2,warningTime))
          }
        }
      }else{
        //不是预警，清空数据
        state.clear()
      }
    }
  }
  //写入redis类
  private class WriteRedis extends RedisMapper[(String,String)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET,"warning30sMachine")
    }

    override def getKeyFromData(t: (String,String)): String = t._1

    override def getValueFromData(t: (String,String)): String = t._2
  }
}
