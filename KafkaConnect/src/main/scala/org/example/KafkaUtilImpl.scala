package org.example

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchemaBuilder, KafkaSink}
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceBuilder}

class KafkaUtilImpl extends KafkaUtil with Serializable {

  override def kafkaRead(topic: String, isNeedEarliestOffset: Boolean): KafkaSource[String] = {
    val kafkaSource: KafkaSourceBuilder[String] = KafkaSource.builder()
      .setBootstrapServers(bootstrapServers)
      .setTopics(topic)
      .setValueOnlyDeserializer(new SimpleStringSchema())
    if (isNeedEarliestOffset) {
      kafkaSource.setStartingOffsets(OffsetsInitializer.earliest()).build()

    } else {
      kafkaSource.setStartingOffsets(OffsetsInitializer.latest()).build()
    }
  }

  override def kafkaWrite(topic: String): KafkaSink[String] = {
    KafkaSink.builder()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder[String]
        .setTopic(topic)
        .setValueSerializationSchema(new SimpleStringSchema)
        .build())
      .build()
  }
}
