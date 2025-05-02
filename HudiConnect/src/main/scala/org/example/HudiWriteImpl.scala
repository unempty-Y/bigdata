package org.example

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.util.Collector
//import org.apache.hudi.util.HoodiePipeline

import scala.collection.mutable.ArrayBuffer
//这里的类一定要序列化!!!!!!!!!  with Serializable接口一定要写!!!!!!!

class HudiWriteImpl extends HudiWrite with Serializable {
  override def hudiWriteTool(columnArr: Array[String], primaryKey: String, partitionField: String, tableName: String, hiveSyncDbName: String, dataStream: DataStream[Array[String]]): Unit = {
//    val builder = HoodiePipeline.builder(tableName)
//    for (elem <- columnArr) {
//      builder.column(s"${elem} varchar(180)")
//    }
//    builder.pk(primaryKey)
//      .partition(partitionField)
//      .option(FlinkOptions.PARTITION_PATH_FIELD.key(), partitionField)
//      .option(FlinkOptions.PATH.key(), s"/${hiveSyncDbName}.db/${tableName}")
//      .option(FlinkOptions.TABLE_NAME.key(), tableName)
//      .option(FlinkOptions.HIVE_SYNC_TABLE.key(), tableName)
//      .option(FlinkOptions.HIVE_SYNC_MODE.key(), "hms")
//      .option(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), "thrift://master:9083")
//      .option(FlinkOptions.HIVE_SYNC_DB.key(), hiveSyncDbName)
//      .option(FlinkOptions.HIVE_SYNC_ENABLED.key(), true)
//    builder.sink(dataStream.map(i => {
//      val data = new GenericRowData(columnArr.length)
//      i.zipWithIndex.map(i => (i._1, i._2)).foreach(it => {
//        data.setField(it._2, StringData.fromString(it._1.toString))
//      })
//      data
//    }), false)
  }
}