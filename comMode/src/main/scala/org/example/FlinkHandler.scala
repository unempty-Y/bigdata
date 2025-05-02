package org.example

import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class FlinkHandler(
                    hbaseWrite: HbaseWrite,
                    hudiWrite: HudiWrite,
                    jdbcWrite: JdbcWrite,
                    kafkaUtil: KafkaUtil,
                    redisWrite: RedisWrite,
                    topNOperator: TopNOperator
                  ) extends HbaseWrite with HudiWrite with JdbcWrite with KafkaUtil with RedisWrite with TopNOperator with Serializable {


  override def hbaseWrite(tbName: String): RichSinkFunction[(String, Array[(String, String, String)])] = hbaseWrite.hbaseWrite(tbName)

  override def hudiWriteTool(columnArr: Array[String], primaryKey: String, partitionField: String, tableName: String, hiveSyncDbName: String, dataStream: DataStream[Array[String]]): Unit = hudiWrite.hudiWriteTool(columnArr, primaryKey, partitionField, tableName, hiveSyncDbName, dataStream)

  override def jdbcRichSink(dbType: String, dbName: String, tbName: String): RichSinkFunction[AnyRef] = jdbcWrite.jdbcRichSink(dbType, dbName, tbName)

  override def kafkaRead(topic: String, isNeedEarliestOffset: Boolean = false): KafkaSource[String] = kafkaUtil.kafkaRead(topic, isNeedEarliestOffset)

  override def kafkaWrite(topic: String): KafkaSink[String] = kafkaUtil.kafkaWrite(topic)

  override def redisSet(key: String): RichSinkFunction[String] = redisWrite.redisSet(key)

  override def redisHSet(key: String): RichSinkFunction[(String, String)] = redisWrite.redisHSet(key)

  override def addElem(key: String, increment: Double, member: String): Unit = topNOperator.addElem(key, increment, member)

  override def getTopN(key: String, start: Int, end: Int, enableAcs: Boolean = false): Array[(String, Double)] = topNOperator.getTopN(key, start, end, enableAcs)

  override def cleanElem(key: String): Unit = topNOperator.cleanElem(key)
}
