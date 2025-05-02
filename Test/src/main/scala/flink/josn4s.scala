package flink

import org.apache.hbase.thirdparty.com.google.gson.JsonParser
import org.apache.spark.sql.execution.streaming.FileStreamSourceOffset.format
import org.json.JSONObject
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson._
import org.json4s.JsonDSL._

object josn4s {
  def main(args: Array[String]): Unit = {
//gson
    val js = """
{"database":"shtd_store","table":"order_master","type":"insert","ts":1670948556,"xid":5625,"commit":true,"data":{"order_sn":"中文1","lineitemquantity":144.0}}
                              """
    val str: JValue = JsonMethods.parse(js, useBigDecimalForDouble = true)
//
//    // 1）单层嵌套取单值
//
//    // 数据类型要一一对应
//    // 方法一：JString模式匹配方式
    val database= (str \ "table")
    //    val string: String = database.toString
    //    println("-------------")
    //    println(string.equals("shtd_store"))
    println(1,database)

    val JString(order_sn) = (str \\ "order_sn")
    println(2,order_sn)

    //将格式转换为标准json
    val JObject(data) = (str \ "data")
    println(3,data)
    //    {"order_sn":"中文1","lineitemquantity":144.0}
    val str1: String = compact(render(data))
    println(4,str1)
//    //需要导入隐式转换
//    ////    0,1,1,2,0
//    import org.json4s.JsonDSL._
//
//    //    login_id (参数1|cans2|...)
//    val strings = str1.replace("(", "").split("|")
    val json1: _root_.org.json4s.JsonAST.JObject =
      ("orderkey" -> 556) ~
        ("cuts_key" -> 32) ~
        ("total" -> 23.9)

    println(5,compact(render(json1)))
//gson
val jsonObject = new JsonParser().parse(js).getAsJsonObject
println(6,jsonObject)
      val jsonObject1 = jsonObject.getAsJsonObject("data")
      println(7,jsonObject1)
      println(8,jsonObject.get("table").getAsString.equals("order_master"))
//    json4s ==>gson
  }
}
