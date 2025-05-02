package gy

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.StateDescriptor.Type
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object D_5_2_1 {
  private val bigdata1 = "192.168.45.5"
  private val BootstrapServer = s"${bigdata1}:28749"

  /**
   *
   * @param change_machine_id   设备id
   * @param last_machine_state  上一状态。即触发本次统计的最近一次非运行状态
   * @param total_change_torun   从其他状态转为运行的总次数
   * @param in_time              flink计算完成时间（yyyy-MM-dd HH:mm:ss）
   */
  case class ChangeData(change_machine_id:Int,last_machine_state:String,total_change_torun:Int,in_time:String)

  def main(args: Array[String]): Unit = {
    //设置执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //设置并行量
    env.setParallelism(1)
    //kafka消费者设置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",BootstrapServer)
    //数据流
    val datastream = env.addSource(new FlinkKafkaConsumer("ChangeRecord", new SimpleStringSchema(), properties))
      .map(data => {
        val arr = data.split(",")
        (arr(1).toInt,arr(3))
      })
      .keyBy(_._1)
      .process(new ChangeProcess)
    //@TODO 输出测试
    datastream.print()
    datastream.addSink(new MysqlWrite)
    //结束执行环境
    env.execute()
  }
  //设置数据处理函数
  private class ChangeProcess extends KeyedProcessFunction[Int,(Int,String),ChangeData]{
    // 状态存储对象，分别存储上一条记录的状态和状态变化总次数
    private var countState: ValueState[Integer] = _    // 状态变化总次数
    private var lastState: ValueState[String] = _      // 上一条记录的状态

    // 初始化状态存储对象
    override def open(parameters: Configuration): Unit = {
      countState = getRuntimeContext.getState(new ValueStateDescriptor("count-state", Types.INT))
      lastState = getRuntimeContext.getState(new ValueStateDescriptor("last-state", Types.STRING))
    }

    override def processElement(value: (Int, String),
                                context: KeyedProcessFunction[Int, (Int, String), ChangeData]#Context,
                                collector: Collector[ChangeData]): Unit = {
      //获取当前变化总数
      var countStateSum = countState.value()
      //获取上一条状态
      val lastStateData = lastState.value()
      //获取当前记录状态
      val newState = value._2
      //判断是否为空 若为空总数赋值为0
      if(countStateSum == null) countStateSum = 0
      // 判断：如果是由 "其他状态->运行"，计数+1。不管什么情况下，都要更新记录状态存储
      newState match {
        case "运行" if("运行" != lastStateData) => {
          countState.update(countStateSum+1)
          lastState.update(lastStateData)
          //输出对象
          collector.collect(ChangeData(value._1,lastStateData,countStateSum+1,new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())))
        }
        case _=> {
          lastState.update(lastStateData)//只更新上一条状态
        }
      }
      }
    private def cleanUp(ctx: KeyedProcessFunction[Int, (Int, String), ChangeData]#Context): Unit = {
      // 清楚所有状态
      lastState.clear()
      countState.clear()
    }
    }
  //写入mysql类
    private class MysqlWrite extends RichSinkFunction[ChangeData]{
    //初始化
    private var con:Connection = _
    private var ps:PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 定义mysql数据库连接url和驱动程序及账号、密码
      val url = "jdbc:mysql://192.168.45.5:49081/shtd_industry?useSSL=false"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val userpwd = "123456"
      Class.forName(driver) // 加载驱动程序
      con = DriverManager.getConnection(url, username, userpwd) // 连接数据库
      // 执行SQL语句
      ps = con.prepareStatement("insert into " +
        "change_state_other_to_run_agg(change_machine_id,last_machine_state,total_change_torun,in_time)" +
        "values(?,?,?,?)")
    }

    override def close(): Unit = {
      super.close()
      if (ps != null) {
        ps.close()
      }
      if (con != null) {
        con.close()
      }
    }

    override def invoke(value: ChangeData): Unit = {
      ps.setInt(1, value.change_machine_id)
      ps.setString(2, value.last_machine_state)
      ps.setInt(3, value.total_change_torun)
      ps.setString(4, value.in_time)
      ps.execute()
    }

  }
  }

