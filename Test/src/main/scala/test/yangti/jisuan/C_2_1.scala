package test.yangti.jisuan



import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.json4s.JsonAST.{JDecimal, JInt, JString}
import org.json4s.jackson.JsonMethods
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.jdk.CollectionConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
/*

 */
object C_2_1 {
  //设定两个变量用于kafka连接
  private val bigdata1 = "192.168.45.16"
  private val bootstrapServers = s"${bigdata1}:9092"
  case class Produce5minAgg(machine_id: String, total_produce: Int)

  def main(args: Array[String]): Unit = {
    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度
    env.setParallelism(1)
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(s"${bigdata1}:9092")
      .setTopics("fact_order_master")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    val dataStream=  env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .map(line=>{
        val str = JsonMethods.parse(line, useBigDecimalForDouble = true)
        val JInt(order_id) = str \\ "order_id"
        val JInt(customer_id) = str \\ "customer_id"
        (order_id.toInt,customer_id.toInt)
      })
    val UV = dataStream
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .aggregate(new AggregateFunction[(Int, Int), (Int, Set[Int]), (Int, Int)] {
        override def createAccumulator(): (Int, Set[Int]) = (0, Set())

        override def add(value: (Int, Int), accumulator: (Int, Set[Int])): (Int, Set[Int]) = {
          val (order_id, user_id) = value
          val (count, set) = accumulator
          (count + 1, set + user_id)
        }

        override def getResult(accumulator: (Int, Set[Int])): (Int, Int) = {
          val (count, set) = accumulator
          (count, set.size) // 返回order_id和UV值
        }

        override def merge(a: (Int, Set[Int]), b: (Int, Set[Int])): (Int, Set[Int]) = {
          val (count1, set1) = a
          val (count2, set2) = b
          (count1 + count2, set1 ++ set2)
        }
      })
      .filter(_._2 > 0) // 过滤掉UV为0的数据
      .map(tuple => (tuple._1, tuple._2))
      .print()

//      .keyBy(_._1)
//      .sum(2)
//      .map(line=>("UV",line._1,line._2))
//
//    val PV = dataStream.map(line=>(line._1,1)).keyBy(_._1).sum(1)
//      .map(line=>("UV",line._1,line._2))
//    val UV_PV = UV.union(PV).print()
//    UV_PV.addSink(new writeHbase(bigdata1, "ads", "pv_uv_result", "info"))
    env.execute("Task3")
  }

  //@TODO 设置写入hbase类
  private class writeHbase(zookeeper: String, namespace: String, table: String, family: String) extends RichSinkFunction[(String,Int,Int)] {
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

    override def invoke(value: (String,Int,Int), context: SinkFunction.Context): Unit = {
      //rowkey:设备id-系统时间
      val rowkey = value._1
      val put = new Put(rowkey.getBytes())
      put.addColumn(family.getBytes(), "order_id".getBytes(), value._2.toString.getBytes())
      put.addColumn(family.getBytes(), value._1.getBytes(), value._3.toString.getBytes())
      htable.put(put)
    }
  }


}
