package org.example

object FlinkHandlerHelper extends Serializable {
  def flinkHandlerHelper(): FlinkHandler = {
    val kafkaUtilImpl = new KafkaUtilImpl
    val redisWriteImpl = new RedisWriteImpl
    val hbaseWriteImpl = new HbaseWriteImpl
    val jdbcWriteImpl = new JdbcWriteImpl
    val hudiWriteImpl = new HudiWriteImpl
    val topNOperatorImpl = new TopNOperatorImpl
    //    val utilProcess = new UtilProcessorImpl
    //    val topNMap = null
    //    val readFromKafkaImpl = null
    new FlinkHandler(hbaseWriteImpl, hudiWriteImpl,jdbcWriteImpl,kafkaUtilImpl, redisWriteImpl, topNOperatorImpl)
  }

}
