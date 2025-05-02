package org.example

import org.apache.flink.streaming.api.datastream.DataStream

trait HudiWrite extends Constant {
def hudiWriteTool(
                   columnArr: Array[String],
                   primaryKey: String,
                   partitionField: String,
                   tableName: String,
                   hiveSyncDbName: String,
                   dataStream: DataStream[Array[String]]
                 ): Unit
}
