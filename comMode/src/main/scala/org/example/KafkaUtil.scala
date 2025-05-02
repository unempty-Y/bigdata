package org.example

import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource

trait KafkaUtil extends Constant {
  val bootstrapServers = s"$bigdata1:$kafkaPort"

  /**
   * @param topic                topic名
   * @param isNeedEarliestOffset 是否从开头读取数据
   * @author
   * @version 2024/5/11 19:31
   * @return String类型数据流
   * @throws
   */
  def kafkaRead(
                     topic: String,
                     isNeedEarliestOffset: Boolean
                   ): KafkaSource[String]

  /**
   * @param topic topic名
   * @param input String数据流
   * @author
   * @version 2024/5/11 19:33
   * @return
   * @throws
   */
  def kafkaWrite(
                    topic: String
                  ): KafkaSink[String]
}
