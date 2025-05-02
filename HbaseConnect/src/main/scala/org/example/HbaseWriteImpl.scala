package org.example

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._

class HbaseWriteImpl extends HbaseWrite with Serializable {

  override def hbaseWrite(tbName: String): RichSinkFunction[(String, Array[(String, String, String)])] = {
    new RichSinkFunction[(String, Array[(String, String, String)])] {
      var connection: Connection = _
      var table: Table = _

      override def open(parameters: Configuration): Unit = {
        val config: conf.Configuration = HBaseConfiguration.create
        config.set(HConstants.ZOOKEEPER_QUORUM, bigdata1)
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort.toString)
        connection = ConnectionFactory.createConnection(config)
        table = connection.getTable(TableName.valueOf(tbName))
      }

      override def invoke(value: (String, Array[(String, String, String)]), context: SinkFunction.Context): Unit = {
        val put = new Put(value._1.getBytes)
        for (i <- value._2) {
          put.addColumn(i._1.getBytes, i._2.getBytes, i._3.getBytes)
        }
        table.put(put)
      }

      override def close(): Unit = {
        table.close()
        connection.close()
      }
    }
  }
}
