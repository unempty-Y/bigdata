package org.example

import java.lang.reflect.Field
import scala.collection.mutable.ArrayBuffer

/**
 * @Param : t classOf[_] 类
 * @Description : 获得类的属性名
 * @Date :
 * @return
 * @throws
 */
/**
* @Param  :
* @Description :  
* @return   
* @Time : 2024/5/11 16:08 
*/

class getAttributeName {
/**
* @Param : T
* @Description :
* @Author :
* @Date : 2024/5/11 16:14 
* @return
* @throws
*/
    /**
        * @param t cassClass
        * @author
        * @version 2024/5/13 10:25
        * @return cassClass和其父类的属性名(String,...)
        * @throws
        */
  def getAllSupperClassFields[T](t: Class[T]): String = {
    //将所有获取到的父类属性加进一个数组中
    val arrayBuffer = new ArrayBuffer[String]()
    var clazz: Class[_ >: T] = t
    while (clazz != null) {
      val fields: Array[Field] = clazz.getDeclaredFields()
      //获取属性名进入Array
      val strings: Array[String] = fields.map(x => x.getName)
      //将每组数据按照逗号分割
      val str: String = strings.addString(new StringBuilder(), ",").toString()
      if (str != "") arrayBuffer += str
      clazz = clazz.getSuperclass
    }
    //最终将所有大字符串数据再次按照逗号分割拼接起来
    arrayBuffer.addString(new StringBuilder(), ",").toString()
  }
    /**
        * @param clazz cassClass
        * @author
        * @version 2024/5/13 10:26
        * @return cassClass的属性名Array[String]
        * @throws
        */
  def getClassFields(clazz: Class[_]): Array[String] = {
    clazz.getDeclaredFields.map(_.getName)
  }
}
