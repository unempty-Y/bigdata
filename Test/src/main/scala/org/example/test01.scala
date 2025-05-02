package org.example

import com.google.gson.{Gson, GsonBuilder}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

object test01 {
  def main(args: Array[String]): Unit = {
    val topic = "ods_mall_data"
    val data = new gsonData
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val handler: FlinkHandler = FlinkHandlerHelper.flinkHandlerHelper()
    val kafkaSource: KafkaSource[String] = handler.kafkaRead(topic, true)
    val ods_mall_data = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), topic)
      .map(i => {
        val gson: Gson = new GsonBuilder().create()
        val value = gson.fromJson(i,classOf[Ods_mall_data])
//        val value = data.getData(i, classOf[Ods_mall_data]).asInstanceOf[Ods_mall_data]
        value
      })
    val order_masterStr = ods_mall_data.filter(_.table.equals("order_master"))
      .map(i => {
        val value = data.getJson(i.data, classOf[Order_master])
        value
      })
    val order_detailStr = ods_mall_data.filter(_.table.equals("order_detail"))
      .map(i => {
        val value = data.getJson(i.data, classOf[Order_detail])
        value
      })
    val order_master = order_masterStr.map(data.getData(_, classOf[Order_master]).asInstanceOf[Order_master])
    val order_detail = order_masterStr.map(data.getData(_, classOf[Order_detail]).asInstanceOf[Order_detail])
    order_masterStr.print()
    order_master.map(_.order_status).print()

//    //    测试写入kafka
//    order_masterStr.sinkTo(handler.kafkaWrite("fact_order_master"))
//    order_detailStr.sinkTo(handler.kafkaWrite("fact_order_detail"))
//
//    //    测试写入hbase
//    val order_master1 = order_master.map(i => {
//      val num: Int = Random.nextInt(10)
//      val name = new getAttributeName
//      val listName: Array[String] = name.getClassFields(classOf[Order_master])
//      val value: Array[String] = i.productIterator.map(_.toString).toArray
//      val tuples: Array[(String, String, String)] = listName.zip(value).map { case (l, v) => ("info", l, v) }
//      (s"$num${i.create_time}", tuples)
//    })
//    order_master1.addSink(handler.hbaseWrite("test:qwe"))
//
//    //    测试写入redis
//    //初始化redis键值
//    handler.cleanElem("gs1top2userconsumptionlist")
//    val top2userconsumption = order_master.map(i => {
//      handler.addElem("gs1top2userconsumptionlist", i.order_money, i.shipping_user)
//      val tuples: Array[(String, Double)] = handler.getTopN("gs1top2userconsumptionlist", 0, 3)
//      s"[${tuples(0)._1}:${tuples(0)._2.formatted("%.2f")}," + (
//        if (tuples.length >= 2)
//          s"${tuples(1)._1}:${tuples(1)._2.formatted("%.2f")}"
//        else "null:0.0"
//        ) + "]"
//    })
//    top2userconsumption.print()
//    top2userconsumption.addSink(handler.redisSet("top2userconsumption"))
//
//    //clickhouse写入测试
//    val refundclick = order_master.filter(_.order_status.equals("已退款")).map(_.asInstanceOf[AnyRef])
//    refundclick.print()
//    refundclick.addSink(handler.jdbcRichSink("clickhouse", "shtd_result", "order_master"))
//
//    //mysql写入测试
//    val refundmysql = order_master.filter(_.order_status.equals("已退款")).map(_.asInstanceOf[AnyRef])
//    refundmysql.print()
//    refundmysql.addSink(handler.jdbcRichSink("mysql", "shtd_result", "order_master"))

    env.execute("test01")
  }
}
