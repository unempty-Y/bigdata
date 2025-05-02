package xl_gy

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

object xl_gy_5 {
  private val bigdata1 = "192.168.45.16"
  private val bootstrapServers = s"${bigdata1}:9092"

  case class RunningData(change_machine_id: Int, last_machine_state: String, total_change_torunning: Int, in_time: String)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment//运行环境
    env.setParallelism(1)//并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//时间语义
    //kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("ChangeRecord", new SimpleStringSchema(), properties))
      .map(data => {
        val arr = data.split(",")
        (arr(1).toInt, arr(3))//id和状态
      })
      .keyBy(_._1)
      //一分钟滚动
      .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
      .process(new RunningProcess())
    //输出测试
    dataStream.print()
    dataStream.addSink(new MysqlWrite)

    env.execute()

  }

  private class MysqlWrite extends RichSinkFunction[RunningData] {
    //连接数据库
    var conn: Connection = _
    //执行sql语句
    var ps: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //定义连接mysql条件
      val url = s"jdbc:mysql://${bigdata1}:3306/shtd_industry?useSSL=false"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "123456"
      Class.forName(driver)
      conn = DriverManager.getConnection(url, username, password) //连接
      //sql语句
      ps = conn.prepareStatement("insert into change_state_other_to_run_agg(change_machine_id,last_machine_state,total_change_torunning,in_time) values(?,?,?,?)")
    }

    override def close(): Unit = {
      super.close()
      if (conn != null) {
        conn.close()
      }
      if (ps != null) {
        ps.close()
      }
    }

    override def invoke(value: RunningData): Unit = {
      //写入数据
      ps.setInt(1, value.change_machine_id)
      ps.setString(2, value.last_machine_state)
      ps.setInt(3, value.total_change_torunning)
      ps.setString(4, value.in_time)
      ps.execute()
    }
  }
  //数据处理

  private class RunningProcess extends ProcessWindowFunction[(Int, String), RunningData, Int, TimeWindow] {
    override def process(key: Int, context: Context, elements: Iterable[(Int, String)], out: Collector[RunningData]): Unit = {
      var last_state = ""//记录上一次的状态
      var state = ""//这一次非运行的状态
      var totalRunning = 0 //起始为0
      for (data <- elements) {
        if (data._2 == "运行" || last_state != "运行") {
          totalRunning += 1
          last_state = data._2
        }
        else if (data._2 != "运行") {
          last_state = data._2
          state = data._2
        }
      }
      //窗口结束时间
      val windowEndTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(context.window.getEnd))
      if (totalRunning > 0) {
        out.collect(RunningData(key, state, totalRunning, windowEndTime))
      }
    }
  }
}