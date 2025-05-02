import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.example.ForceTable;

public class ReadFromMySQL {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 JDBC 输入格式
        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.31.128:3306/fanlidatabase?useSSL=false")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("SELECT * FROM forcetable")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.INT_TYPE_INFO, // forceid
                        BasicTypeInfo.STRING_TYPE_INFO, // abutmentno
                        BasicTypeInfo.STRING_TYPE_INFO, // bridgeid
                        BasicTypeInfo.STRING_TYPE_INFO, // piereno
                        BasicTypeInfo.FLOAT_TYPE_INFO, // forcevalue
                        BasicTypeInfo.STRING_TYPE_INFO  // time_stamp
                ))
                .finish();

        // 读取数据
        SingleOutputStreamOperator<ForceTable> map = env.createInput(jdbcInputFormat)
                .map(new MapFunction<Row, ForceTable>() {
                    @Override
                    public ForceTable map(Row row) throws Exception {
                        return new ForceTable((String) row.getField(1),
                                (String) row.getField(2),
                                (String) row.getField(3)
                                , (Float) row.getField(4)
                                , (String) row.getField(5)
                        );
                    }
                });

        // 打印结果
        map.print();

        // 执行程序
        env.execute("Flink Read from MySQL");
    }
}
