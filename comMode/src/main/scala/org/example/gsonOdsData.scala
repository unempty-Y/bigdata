package org.example

import com.google.gson.Gson
import org.apache.flink.table.planner.JMap

class gsonOdsData(in: String) {
  private val gson = new Gson
  private val ods_mall_data: Ods_mall_data = gson.fromJson(in, classOf[Ods_mall_data])
  private var data= ods_mall_data.data match {
    case detail: JMap[String, Any] if ods_mall_data.table == "order_detail" =>
      gson.fromJson(gson.toJson(detail), classOf[Order_detail])
    case master: JMap[String, Any] if ods_mall_data.table == "order_master" =>
      gson.fromJson(gson.toJson(master), classOf[Order_master])
    case _=>None
  }
  def getData:Option[Any]=Option(data)
}
