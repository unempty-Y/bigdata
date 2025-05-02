package org.example

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.api.functions.sink._
trait RedisWrite extends Constant {
  /**
   * @param key 键值
   * @param input(String) 值
   * @author
   * @version 2024/5/12 22:30
   * @return 在redis使用get key获取值
   * @throws 如遇乱码 redis-cli --raw
   */
  def redisSet(key: String): RichSinkFunction[String]
  /**
   * @param key 键值
   * @param input (String,String) 字段，值
   * @author
   * @version 2024/5/12 22:31
   * @return 在redis使用Hgetall key获取值
   * @throws 如遇乱码 redis-cli --raw
   */
  def redisHSet(key: String): RichSinkFunction[(String, String)]
}