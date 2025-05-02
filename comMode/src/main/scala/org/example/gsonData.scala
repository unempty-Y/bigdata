package org.example

import com.google.gson.Gson

class gsonData extends Serializable {
  /**
   * @param in    输入要转换的json数据
   * @param clazz 输入json转换后的cass类型
   * @author
   * @version 2024/5/15 11:16
   * @return 转换后的cass类
   * @throws
   */
  def getData(in: String, clazz: Class[_]): Any = {
    val gson = new Gson

    val data = gson.fromJson(in, clazz)
    data
  }

  /**
   * @param in    输入要转换的数据
   * @param clazz cass类，规范转换后的json数据
   * @author
   * @version 2024/5/15 11:18
   * @return 转换后的json数据
   * @throws
   */
  def getJson(in: Any, clazz: Class[_]): String = {
    val gson = new Gson
    val value: Any = gson.fromJson(gson.toJson(in), clazz)
    gson.toJson(value)
  }
}
