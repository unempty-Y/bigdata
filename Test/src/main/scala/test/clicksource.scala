import java.sql.{Connection, DriverManager, ResultSet, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date

object clicksource {
  private val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
  private val SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT)

  def main(args: Array[String]): Unit = {
    val url = "192.168.45.16" // 云数据库ClickHouse的VPC地址或外网地址
    val username = "default" // 云数据库ClickHouse的账号
    val password = "123456" // 云数据库ClickHouse的密码

    val connectionStr = s"jdbc:clickhouse://$url:8123/default"

    val connection = DriverManager.getConnection(connectionStr, username, password)
    val stmt = connection.createStatement()

//    {
//      val createTableDDL = "create table test_table" +
//        "(id UInt32, " +
//        "dt_str String, " +
//        "dt_col DateTime) " +
//        "engine=MergeTree()" +
//        "partition by toYYYYMM(dt_col)" +
//        "order by (id)" +
//        "primary key (id)" +
//        "sample by (id)" +
//        "settings index_granularity = 8192;"
////      CREATE TABLE test_table (
////        id UInt32,
////        dt_str String,
////        dt_col DateTime
////      ) ENGINE = MergeTree()
////      ORDER BY id;
//      stmt.execute(createTableDDL)
//      println("create local table done.")
//    }

    {
      val createTableDDL = "create table test_dist  " +
        "as default.test_table "
//        "ENGINE = MergeTree();"
      stmt.execute(createTableDDL)
      println("create distributed table done")
    }

    println("write 100000 rows...")
    val startTime = System.currentTimeMillis()

    // Write 10 batch
    for (batch <- 0 until 10) {
      val sb = new StringBuilder()

      // Build one batch
      sb.append(s"insert into test_dist values(${batch * 10000}, '2020-02-19 16:00:00', '2020-02-19 16:00:00')")
      for (row <- 1 until 10000) {
        sb.append(s", (${batch * 10000 + row}, '2020-02-19 16:00:00', '2020-02-19 16:00:00')")
      }

      // Write one batch: 10000 rows
      stmt.execute(sb.toString())
    }

    val endTime = System.currentTimeMillis()
    println(s"total time cost to write 10W rows: ${endTime - startTime}ms")

    Thread.sleep(2 * 1000)

    println("Select count(id)...")
    val rs1 = stmt.executeQuery("select count(id) from test_dist")
    while (rs1.next()) {
      val count = rs1.getInt(1)
      println(s"id count: $count")
    }

    val rs2 = stmt.executeQuery("select id, dt_str, dt_col from test_dist limit 10")
    while (rs2.next()) {
      val id = rs2.getInt(1)
      val dateStr = rs2.getString(2)
      val time = rs2.getTimestamp(3)

      val defaultDate = SIMPLE_DATE_FORMAT.format(new Date(time.getTime))
      println(s"id: $id, date_str: $dateStr, date_col: $defaultDate")
    }
  }
}