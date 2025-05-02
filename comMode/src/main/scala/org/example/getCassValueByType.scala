package org.example

import org.apache.spark.sql.SparkSession

class getCassValueByType {
  /**
   * @param instance 输入含值的CassClass
   * @author
   * @version 2024/5/12 20:26
   * @return (String=>'String',Int,Double,Boolean)
   * @throws
   */

  def processInstance(instance: AnyRef): String = {
    // 获取实例的 Class 对象
    val clazz = instance.getClass
    // 获取类的所有字段（包括继承的字段）
    val fields = clazz.getDeclaredFields
    var put = "("
    fields.foreach { field =>
      // 设置字段为可访问，即使它们是私有的
      field.setAccessible(true)
      // 获取字段的值
      val value = field.get(instance)

      if (put != "(") put += ","
      // 根据值的类型进行不同的处理
      value match {
        case s: String => put += s"'$s'"
        case i: Integer => put += i
        case b: java.lang.Boolean => put += b
        case c: java.lang.Character => put += c
        case d: java.lang.Double => put += d
//        case other => println(s"Unknown field type: ${other.getClass.getName}")
      }
    }
    put += ")"
    put
  }
}
