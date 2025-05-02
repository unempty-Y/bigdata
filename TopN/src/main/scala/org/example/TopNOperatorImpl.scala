package org.example

import java.util
import redis.clients.jedis.{Jedis, Tuple}

import scala.collection.JavaConverters

class TopNOperatorImpl extends TopNOperator with Serializable {

  override def addElem(key: String, increment: Double, member: String): Unit = {
    val jedis = new Jedis(bigdata1, redisPort)
    //把要增加的值传入key下的成员中并相加
    jedis.zincrby(key, increment, member)
  }

  override def getTopN(key: String, start: Int, end: Int, enableAcs: Boolean): Array[(String, Double)] = {
    val jedis = new Jedis(bigdata1, redisPort)
    //初始化变量存储数据
    var data: util.Set[Tuple] = null
    if (enableAcs) { //升序排序
      data = jedis.zrangeWithScores(key, start, end)
    } else { //降序排序
      data = jedis.zrevrangeWithScores(key, start, end)
    }
    //使用Java转换器将scala数组转换为Java数组
    JavaConverters.asScalaSet(data)
      .toArray
      .map(i => (i.getElement, i.getScore))
  }

  override def cleanElem(key: String): Unit = {
    val jedis = new Jedis(bigdata1, redisPort)
    jedis.del(key)
  }
}
