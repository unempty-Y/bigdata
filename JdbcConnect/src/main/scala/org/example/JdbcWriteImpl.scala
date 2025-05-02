package org.example

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, Statement}
import scala.reflect.runtime.universe._

class JdbcWriteImpl extends JdbcWrite with Serializable {

  override def jdbcRichSink(dbType: String, dbName: String, tbName: String): RichSinkFunction[AnyRef] = {
    new RichSinkFunction[AnyRef] {
      //根据数据库类型建立联系
      private val port = if (dbType.equals("mysql")) mysqlPort else clickhousePort
      private val user = if (dbType.equals("mysql")) mysqlUser_Password._1 else clickhouseUser_Password._1
      private val password = if (dbType.equals("mysql")) mysqlUser_Password._2 else clickhouseUser_Password._2
      private val connectionStr = s"jdbc:$dbType://$bigdata1:$port/$dbName" + (if (dbType.equals("mysql")) "?useSSL=false" else "")

      private var connection: Connection = _
      private var statement: Statement = _

      override def open(parameters: Configuration): Unit = {
        connection = DriverManager.getConnection(connectionStr, user, password)
        statement = connection.createStatement()
      }

      override def invoke(value: AnyRef, context: SinkFunction.Context): Unit = {
        val byType = new getCassValueByType
        val sql = s"insert into $tbName values " + byType.processInstance(value)
        statement.execute(sql)
      }

      override def close(): Unit = {

        statement.close()
      }
    }

  }
}
