package org.example

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction


trait HbaseWrite extends Constant {


  /**
   * @param tbName 命名空间：表名 or 表名
   * @param input  rowKey,(family,列名,value)
   * @author
   * @version 2024/5/13 16:55
   * @return
   * @throws
   */
  def hbaseWrite(
                  tbName: String
                ): RichSinkFunction[(String, Array[(String, String, String)])]
}
