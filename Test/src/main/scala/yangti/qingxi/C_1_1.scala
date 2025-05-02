import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.json4s
import org.json4s.JValue
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods

import java.util.{Date, Properties}

object C_1_1 {
  private val bigdata1 = "192.168.45.16"
  private var table = ""
  def main(args: Array[String]): Unit = {

    // 设置流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置使用处理时间
    import org.apache.flink.streaming.api.TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 设置并行度
    env.setParallelism(1)

    // 启用检查点
    env.enableCheckpointing(5000)

    // kafka source
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(s"${bigdata1}:9092")
      .setTopics("ods_mall_data")
      //      .setGroupId("group-test")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    // kafka sink
    val properties = new Properties()
    properties.setProperty("trans.timeout.ms", "7200000") // 2 hours

    // KafkaSink 允许将记录流写入一个或多个 Kafka 主题。
    val kafkaSink = KafkaSink.builder[String]
      .setBootstrapServers(s"${bigdata1}:9092")
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String] //.builder()
        .setTopicSelector(new TopicSelector[String] {
          override def apply(t: String): String = {
            if (t.contains("order_id")) "fact_order_master"
            else if (t.contains("order_detail")) "fact_order_detail"
            //            else if (t.contains("XXX")) "XXX"
            else null
          }
        })
        .setValueSerializationSchema(new SimpleStringSchema)
        .build()).build()

    val data= env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line => line.contains("order_master") || line.contains("order_detail"))
      .map(line => {
        import com.google.gson.JsonParser
        val jsonobj = new JsonParser().parse(line).getAsJsonObject
        jsonobj.getAsJsonObject("data").toString
      })
    val tablename = data.map(json => {
      val str: JValue = JsonMethods.parse(json, useBigDecimalForDouble = true)
      val JString(order_sn) = (str \\ "")
      table = order_sn
      table
    })
data.print()
    val Hbase1: DataStream[String] = data
      .process(new chosetable)
    Hbase1.addSink(new writeHbase(bigdata1, "test", table, "info"))

      data.sinkTo(kafkaSink)

    env.execute("Task1")
  }
class chosetable extends ProcessFunction[String,String] {
  override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
    if (i.contains("order_id")) table="order_master"
    else if (i.contains("order_detail")) table="order_detail"
    //            else if (t.contains("XXX")) "XXX"
    collector.collect(i)
  }
}
  private class writeHbase(zookeeper: String, namepace: String, table: String, family: String) extends RichSinkFunction[String] {
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

    override def invoke(value: String, context: SinkFunction.Context): Unit = {
      val str: JValue = JsonMethods.parse(value, useBigDecimalForDouble = true)
      val rowkey = (str \ "order_id").toString
      val put = new Put(rowkey.getBytes())
      put.addColumn(family.getBytes(), "order_id".getBytes(),value.toString.getBytes())
//      put.addColumn(family.getBytes(), "xxx".getBytes(),(str \ "XXXX").toString.getBytes())
//      put.addColumn(family.getBytes(), "xxx".getBytes(),(str \ "XXXX").toString.getBytes())
//      put.addColumn(family.getBytes(), "xxx".getBytes(),(str \ "XXXX").toString.getBytes())
      htable.put(put)
    }
  }
}