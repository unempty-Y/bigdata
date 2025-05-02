package xl_ds

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, TopicSelector}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.TableName

import java.util.Properties

object ds_2_3 {

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
      .setBootstrapServers("192.168.45.16:9092")
      .setTopics("ods_mall_data")
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      //      .setGroupId("group-test")
      .setStartingOffsets(OffsetsInitializer.earliest)
      .setValueOnlyDeserializer(new SimpleStringSchema)
      .build()

    // kafka sink
    val properties = new Properties()
    properties.setProperty("trans.timeout.ms", "7200000") // 2 hours

    // KafkaSink 允许将记录流写入一个或多个 Kafka 主题。
    val kafkaSink = KafkaSink.builder[String]
      .setBootstrapServers("192.168.45.16:9092")
      .setKafkaProducerConfig(properties)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String] //.builder()
        .setTopicSelector(new TopicSelector[String] {
          override def apply(t: String): String = {
            if (t.contains("order_id")) "fact_order_master"
            else if (t.contains("order_detail")) "fact_order_detail"
            else if (t.contains("customer_info")) "dim_customer_info"
            else if (t.contains("product_info")) "dim_product_info"
            else null
          }
        })
        .setValueSerializationSchema(new SimpleStringSchema)
        .build()).build()

    val dataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks[String], "kafka source")
      .filter(line =>
        line.contains("order_master") ||
          line.contains("order_detail") ||
          line.contains("customer_info") || line.contains("product_info")
      )
      .map(line => {
        import com.google.gson.JsonParser
        val jsonobj = new JsonParser().parse(line).getAsJsonObject
        jsonobj.getAsJsonObject("data").toString
      })
//      .map(line=>s"data:$line")
//    dataStream.print()
    val fact_order_detail = dataStream.filter(line=>line.contains("order_id"))
      fact_order_detail.print
//      dataStream.sinkTo(kafkaSink)
    env.execute("Task1")
  }
  import org.apache.flink.api.common.io.OutputFormat
  import org.apache.flink.api.java.tuple.Tuple2
  import org.apache.hadoop.hbase.client.{Put, Connection, ConnectionFactory, Table}
  import org.apache.hadoop.hbase.util.Bytes

  class HBaseMultipleOutputFormat extends OutputFormat[Tuple2[String, String]] {

    @transient private var connection: Connection = _
    @transient private var table1: Table = _
    @transient private var table2: Table = _

    override def configure(parameters: Configuration): Unit = {
      // 初始化HBase连接
      connection = ConnectionFactory.createConnection()
      table1 = connection.getTable(TableName.valueOf("table1"))
      table2 = connection.getTable(TableName.valueOf("table2"))
    }

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Tuple2[String, String]): Unit = {
      val key = record.f0
      val value = record.f1

      val put1 = new Put(Bytes.toBytes(key))
      put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(value))
      table1.put(put1)

      val put2 = new Put(Bytes.toBytes(key))
      put2.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("col2"), Bytes.toBytes(value))
      table2.put(put2)
    }

    override def close(): Unit = {
      // 关闭HBase连接
      table1.close()
      table2.close()
      connection.close()
    }
  }

}