package gy

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object D_4_3_1 {
  private val bigdata1 = "192.168.45.5"
  private val bootstrapServers = s"${bigdata1}:28749"
  /**
   *
   * @param change_machine_id 设备id
   * @param totalwarning 未被处理预警的数据总数
   * @param window_end_time 窗口结束时间
   */
  case class WarningData(change_machine_id: Int, totalwarning: Int, window_end_time: String)

  def main(args: Array[String]): Unit = {
    //设置环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //设置kakfa消费者
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    val dataStream = env.addSource(new FlinkKafkaConsumer("ChangeRecord", new SimpleStringSchema(), properties))
      .map(data => {
        val arr = data.split(",")
        (arr(1).toInt, arr(3))
      })
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.minutes(3)))
      .process(new WarningProcess())
    //@TODO 测试数据
    dataStream.print()
    //写入mysql中
    dataStream.addSink(new MysqlWrite)
    //结束环境
    env.execute()
  }
  //mysql写入类
  private class MysqlWrite extends RichSinkFunction[WarningData]{
    //连接数据库用的
    var conn: Connection = _
    //执行sql语句
    var ps:PreparedStatement = _
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //定义连接mysql条件
      val url = s"jdbc:mysql://${bigdata1}:49081/shtd_industry?useSSL=false"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "123456"
      Class.forName(driver)
      conn = DriverManager.getConnection(url, username, password)//链接数据库
      //sql语句
      ps = conn.prepareStatement("insert into threemin_warning_state_agg(change_machine_id,asctotalwarning,window_end_time) values(?,?,?)")
    }
    override def close(): Unit = {
      super.close()
      if(conn != null){
        conn.close()
      }
      if(ps != null){
        ps.close()
      }
    }
    override def invoke(value: WarningData): Unit = {
      //写入数据
      ps.setInt(1,value.change_machine_id)
      ps.setInt(2,value.totalwarning)
      ps.setString(3,value.window_end_time)
      ps.execute()
    }
  }
  //数据处理
  /**
   * 泛型参数(Int, String, String)，表示一个三元组，其中第一个元素是设备ID，第二个元素是设备状态，第三个元素是时间戳。
   * 泛型参数WarningData表示输出元素的类型，即预警数据的类型。
   * 泛型参数Int表示键的类型，即设备ID的类型。
   * 泛型参数TimeWindow表示窗口的类型，表示时间窗口。
   */
  private class WarningProcess extends ProcessWindowFunction[(Int,String),WarningData,Int,TimeWindow] {
    override def process(key: Int, context: Context, elements: Iterable[(Int, String)], out: Collector[WarningData]): Unit = {

      var totalWarning = 0 //总数设为0
      for (data <- elements) {
        //为预警状态总数+1
        if (data._2 == "预警") {
          totalWarning += 1
        }
        //不预警状态总数-1
        else if (data._2 == "运行" || data._2 == "停止") {
          totalWarning -= 1
        }
      }
      //窗口结束时间
      val windowEndTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(context.window.getEnd))
      if (totalWarning > 0) {
        out.collect(WarningData(key, totalWarning, windowEndTime))
      }
    }
  }
}