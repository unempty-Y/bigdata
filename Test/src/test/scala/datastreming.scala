//import com.ibm.icu.impl.locale.KeyTypeData.(String, Double)

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object datastreming {
  def main(args: Array[String]): Unit = {
    //流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lineData: DataStream[String] = env.readTextFile("input/hello.txt")
    //文本流
    //        val lineData: DataStream[String] = env.socketTextStream("192.168.45.21",4567)
    //    val parameterTool = ParameterTool.fromArgs(args)
    //    val hostname = parameterTool.get("host")
    //    val hostname = "192.168.45.16"
    //    val port = parameterTool.getInt("port")
    //    val port = 9092
    //    val lineData: DataStream[String] = env.socketTextStream(hostname, port)


    val value = lineData
      .map(line => {
        val arr = line.split(",")
        (arr(0).toLong, arr(1).toDouble)
      })
            .keyBy(a => true)
      //.map((1,_))
//      .keyBy(_._1)
      .process(new MaxAmount)
//            .map(a=>s"[${a(0)},${if (a.length >2) a(1) else null},${if (a.length >3) a(2) else null}]")
.map(a=>s"[${a(0)},${a(1)},${ a(2) }]")

      .print()
    env.execute("test")
  }

  class MyOrderProcessFunction extends KeyedProcessFunction[String, (String, Double), (String, Double)] {

    // lazy val初始化资源占用少，只初始化一次
    lazy val lastOrderAmount: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastAmount", classOf[Double]))

    // open这样初始化每次都重新初始化
    //    private var lastOrderAmount: ValueState[Double] = _
    //    override def open(parameters: Configuration): Unit = {
    //      lastOrderAmount = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastAmount", classOf[Double]))
    //
    //    }

    // 处理每一个元素，累加总金额
    override def processElement(input: (String, Double),
                                context: KeyedProcessFunction[String, (String, Double), (String, Double)]#Context,
                                out: Collector[(String, Double)]): Unit = {
      // 将当前订单记录的金额累加到上一次的总金额上
      var accAmount = lastOrderAmount.value() + input._2
      // 将当前总金额发送到下游
      out.collect(input._1, accAmount)
      // 并更新状态
      lastOrderAmount.update(accAmount)
    }
  }

  class MaxAmount extends ProcessFunction[(Long, Double), List[String]] {
    lazy val valueStateList: ListState[(Long, Double)] = getRuntimeContext.getListState(new ListStateDescriptor[(Long, Double)]("valueStateList", classOf[(Long, Double)]))

    override def processElement(i: (Long, Double), context: ProcessFunction[(Long, Double), List[String]]#Context, collector: Collector[List[String]]): Unit = {
      valueStateList.add(i)
      val Nulls: Array[String] = Array("Null", "NUll", "NULL")
      import scala.collection.JavaConverters._
      val valueList = valueStateList.get()
        .asScala
        .toList
        //根据商品Id分组累加
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
        //根据商品销售总额排序
        .sortBy(_._2).reverse
      valueStateList.clear()
      valueStateList.addAll(valueList.asJava)
            val output = valueList.map(line => s"${line._1}:${line._2}"):+"null":+"null"
      collector.collect(output)
    }
  }
}
