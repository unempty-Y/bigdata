
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object writeHbase {
  def main(args: Array[String]): Unit = {
    //hbase连接配置
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.45.16")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    //创建hbase连接
    val connection = ConnectionFactory.createConnection(conf)

    try{
      //获取hbase表
      val table = connection.getTable(TableName.valueOf("lncc")) //表名
      //创建put对象插入数据
      val put = new Put(Bytes.toBytes("3"))  //行键
      put.addColumn(
        Bytes.toBytes("info"), //列族
        Bytes.toBytes("id"), //列名
        Bytes.toBytes("2")  //数值
      )
      put.addColumn(
        Bytes.toBytes("info"), //列族
        Bytes.toBytes("name"), //列名
        Bytes.toBytes("eww")  //数值
      )
      //将数据写入表中
      table.put(put)
      //关闭table
      table.close()
    }
    //测试输出代码(可有可无)
    println("成功插入数值")
    //关闭hbase连接
    connection.close()

  }
}