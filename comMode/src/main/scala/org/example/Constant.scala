package org.example

trait Constant {
  //  val bigdata1 = "192.168.45.16"
  //  val bigdata2 = "192.168.45.37"
  //  val bigdata3 = "192.168.45.38"
  val bigdata1 = "bigdata1"
  val bigdata2 = "bigdata2"
  val bigdata3 = "bigdata3"
  val kafkaPort = 9092
  val zookeeperPort = 2181
  val redisPort = 6379
  val mysqlPort = 3306
  val mysqlUser_Password = ("root", "123456")
  val clickhousePort = 8123
  val clickhouseUser_Password = ("default", "123456")
  val hudiPort = 9083
  val filter = Array("已发货", "已签收", "已付款") // "已发货", "已付款", "已下单","已签收","已退款"

}