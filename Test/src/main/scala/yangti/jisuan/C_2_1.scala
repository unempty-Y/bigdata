
import org.apache.flink.api.common.functions.ReduceFunction

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods

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
    //用properties来保存连接kafka的相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    val dataStream= env.addSource(new FlinkKafkaConsumer[String]("Topic", new SimpleStringSchema(), properties))
      .map(line=>{
        val value = JsonMethods.parse(line, useBigDecimalForDouble = true)
        ((value \ "id").toString.toInt,(value \ "user_id").toString.toInt)
      })

    val UV = dataStream
      .keyBy(_._1)
      .windowAll(TumblingProcessingTimeWindows.of(Time.days(1)))
      //根据key去重统计
      .reduce(new ReduceFunction[(Int,Int)] {
      override def reduce(t: (Int,Int), t1: (Int,Int)): (Int,Int) = {
        if (t._1 == t1._1 && t._2 != t1._2)
          (t._1, 1)
        else (t._1, 0)
      }})
      .keyBy(_._1)
      .sum(1)
      .map(line=>("UV",line._1,line._2))


    val PV = dataStream.map(line=>(line._1,1)).keyBy(_._1).sum(1)
      .map(line=>("UV",line._1,line._2))
    val UV_PV = UV.union(PV)
    UV_PV.addSink(new writeHbase(bigdata1, "ads", "online_uv_pv", "info"))
    env.execute()
  }

  //@TODO 设置写入hbase类
  private class writeHbase(zookeeper: String, namepace: String, table: String, family: String) extends RichSinkFunction[(String,Int,Int)] {
    private var connection: Connection = _
    private var htable: Table = _

    override def open(parameters: Configuration): Unit = {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)
      hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      connection = ConnectionFactory.createConnection(hbaseConf)
      htable = connection.getTable(TableName.valueOf(namepace, table))
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
      val rowkey = s"${value._1}"
      val put = new Put(rowkey.getBytes())
      put.addColumn(family.getBytes(),value._2.toString.getBytes(), value._3.toString.getBytes())
      put.addColumn(family.getBytes(),value._2.toString.getBytes(), value._3.toString.getBytes())
      htable.put(put)
    }
  }


}
