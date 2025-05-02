package xl_ds

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object C_1_1 {
  private val bigdata1 = "192.168.45.16"
  private val BootstrapServers = s"${bigdata1}:9092"
  def main(args: Array[String]): Unit = {
    val env= StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
      //检查点
    env.enableCheckpointing(5000)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",BootstrapServers)
    val value = env.addSource(new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), properties))
//      .map()

    env.execute("test")
  }
}
