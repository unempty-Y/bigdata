package gy

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

/*

 */
object D_5_1 {
  //设定两个变量用于kafka连接
  private val bigdata1 = "192.168.45.16"
  private val bootstrapServers = s"${bigdata1}:9092"
  case class Produce5minAgg(machine_id: String, total_produce: Int)

  @transient private var globalVariableState: String = null
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
    //@TODO 读取kafka中的数据
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("ProduceRecord", new SimpleStringSchema(), properties))
    //@TODO map数据转换操作
    //提取设备id、生产数据
    val HbaseData: DataStream[Produce5minAgg] = dataStream
      //只保留已检验的数据
      .filter(data => {
        val arr = data.split(",")
        arr(9) == "1"
      })
      //抽取要处理的数据
      .map(data => {
        val arr = data.split(",")
        Produce5minAgg(arr(1), arr(2).toInt)
      })
      //选取machine_id字段作为分组取键
      .keyBy(_.machine_id)
      //设置五分钟延迟窗口
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      //对Produce5minAgg中的total_produce字段进行求和
      .sum("total_produce")
    //@TODO 输出测试
    HbaseData.print()
    //@TODO 写入kafka ProduceRecord_01中
    // dataStream.addSink(new FlinkKafkaProducer[String](bootstrapServers,"ProduceRecord_01",new SimpleStringSchema()))
    //@TODO 写入Hbase中
    HbaseData.addSink(new writeHbase(bigdata1, "gyflinkresult", globalVariableState, "info"))
    env.execute()
  }

  //@TODO 设置写入hbase类
  private class writeHbase(zookeeper: String, namepace: String, table: String, family: String) extends RichSinkFunction[Produce5minAgg] {
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

    override def invoke(value: Produce5minAgg, context: SinkFunction.Context): Unit = {
      //rowkey:设备id-系统时间
      val id = value.machine_id
      val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date())
      val rowkey = s"${id}-${time}"
      val put = new Put(rowkey.getBytes())
      put.addColumn(family.getBytes(), "machine_id".getBytes(), value.machine_id.getBytes())
      put.addColumn(family.getBytes(), "total_produce".getBytes(), value.total_produce.toString.getBytes())
      htable.put(put)
    }
  }


}
