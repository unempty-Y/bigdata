package org.example

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import redis.clients.jedis.Jedis

class RedisWriteImpl extends RedisWrite with Serializable {

  override def redisSet(key: String): RichSinkFunction[String] = {
    new RichSinkFunction[String] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = new Jedis(bigdata1, redisPort)
      }

      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        jedis.set(key, value)
      }

      override def close(): Unit = jedis.close()
    }
  }

  override def redisHSet(key: String): RichSinkFunction[(String, String)] = {
    new RichSinkFunction[(String, String)] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = new Jedis(bigdata1, redisPort)
      }

      override def invoke(value: (String, String), context: SinkFunction.Context): Unit = {
        jedis.hset(key, value._1, value._2)
      }

      override def close(): Unit = jedis.close()
    }
  }
}
