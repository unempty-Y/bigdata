package org.example

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import java.sql.DriverManager

trait JdbcWrite extends Constant {
  /**
   * @param columnLen 输入变量数
   * @param dbType    数据库类型mysql或clickhouse
   * @param dbName    数据库名
   * @param tbName    表名
   * @param input     支持任意类型cassClass值
   * @author a
   * @version 2024/5/11 19:08
   * @return
   * @throws
   */

  def jdbcRichSink(
                    dbType: String,
                    dbName: String,
                    tbName: String
                  ): RichSinkFunction[AnyRef] {
  }
}
