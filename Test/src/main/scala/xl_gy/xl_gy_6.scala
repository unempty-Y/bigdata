package xl_gy

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Properties

object xl_gy_6 {
  private val bigdata1 = "192.168.45.16"
  private val bootstrapServer = s"${bigdata1}:9092"
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",bootstrapServer)
    val datastream = env.addSource(new FlinkKafkaConsumer("EnvironmentData", new SimpleStringSchema(), properties))
      .map(data =>{
        val arr = data.split(",")
        //获取时间戳
        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr(10)).getTime
        (arr(1),arr(5).toDouble,time)
      })
        .assignAscendingTimestamps(_._3)
        .keyBy(_._1)
        .process(new WarningProcess)
    //@TODO 测试数据
    datastream.print()
    //写入redis
    val config = new FlinkJedisPoolConfig.Builder()
      .setHost(bigdata1)
      .setPort(6379)
      .build()
    datastream.addSink(new RedisSink[(String, String)](config,new RedisWrite))

    env.execute()
  }
  //处理
  private class WarningProcess extends KeyedProcessFunction[String,(String,Double,Long),(String,String)]{
    //设置存储器（温度，时间戳）
    lazy val stateAndTime: ValueState[(String, Long)] = getRuntimeContext.getState(new ValueStateDescriptor[(String,Long)]("State-Time",classOf[(String,Long)]))
    override def processElement(data: (String, Double, Long), context: KeyedProcessFunction[String, (String, Double, Long), (String, String)]#Context, collector: Collector[(String, String)]): Unit = {
      //获取上次时间和温度
      val newstateAndTime = stateAndTime.value()
      //获取当前时间
      val newTime = data._3
      //获取当前温度
      val newState = data._2
      //判断如果当前温度高于38度则输出结果
      if (newState >= 38 ){
        if(newstateAndTime != null){
          //获取上次时间
          val lastTime = newstateAndTime._2
          //判断持续3分钟高温 就报警
          if(newTime - lastTime >= 1800000){
            collector.collect((data._1,s"${data._1}-${new SimpleDateFormat("yyy-MM-dd HH:mm:ss").format(newTime)},设备${data._1}连续三分钟温度高于38度请及时处理！"))
            stateAndTime.update((newState.toString,newTime))
          }
        }
        else {
          stateAndTime.update((newState.toString,newTime))
        }
      }
      else {
        //不是预警，清空数据
        stateAndTime.clear()
      }
    }
  }
  private class RedisWrite extends RedisMapper[(String,String)] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "env_temperature_monitor")
    }

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2
  }
}
