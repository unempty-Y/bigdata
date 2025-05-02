package org.example

import com.ibm.icu.text.SimpleFormatter
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat

class choseEventTime extends BoundedOutOfOrdernessTimestampExtractor[AnyRef](Time.seconds(5)) {
  val timeFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  override def extractTimestamp(t: AnyRef): Long = {
    if (t.getClass == Order_master)
      timeFormat.parse(t.asInstanceOf[Order_master].create_time).getTime
    else if (t.getClass == Order_detail)
      timeFormat.parse(t.asInstanceOf[Order_detail].create_time).getTime
    else {
      print("时间出现错误")
      10000L
    }

  }
}
