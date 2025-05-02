package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;


public class readKafkaToMysql {
    private static final String host =
            "192.168.31.128";
    //            "127.0.0.1"; // 目标服务器IP地址
    private static final int server_port = 40126; // 目标服务器端口号
    //mysql账号
    private static final String user = "root";
    private static final String password = "123456";
    private static final String databases = "fanlidatabase";

    public static void main(String[] args) throws Exception {
        String topic = "ada";

        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic,host+":"+9092);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.31.128:3306/fanlidatabase?useSSL=false")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("SELECT * FROM forcetable where bridgeid != 01")
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.INT_TYPE_INFO, // forceid
                        BasicTypeInfo.STRING_TYPE_INFO, // abutmentno
                        BasicTypeInfo.STRING_TYPE_INFO, // bridgeid
                        BasicTypeInfo.STRING_TYPE_INFO, // piereno
                        BasicTypeInfo.FLOAT_TYPE_INFO, // forcevalue
                        BasicTypeInfo.STRING_TYPE_INFO  // time_stamp
                ))
                .finish();
        SingleOutputStreamOperator<ForceTable> Source = env.createInput(jdbcInputFormat)
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

//        SingleOutputStreamOperator<ForceTable> Source = env
//                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
//                .map(i -> {
//                    Gson gson = new GsonBuilder().create();
//                    return gson.fromJson(i, ForceTable.class);
//                });

        SingleOutputStreamOperator<ForceTable> watermakerSource = Source
                .filter(i -> i.getForcevalue() >= 0)//初步清洗
                .assignTimestampsAndWatermarks(forceTableWatermarkStrategy());

        watermakerSource.print();
        watermakerSource.addSink( JdbcWriter(databases));
        env.execute("countRateRedis");

    }

    public static WatermarkStrategy<ForceTable> forceTableWatermarkStrategy() {
        return WatermarkStrategy.<ForceTable>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ForceTable>() {
                    @Override
                    public long extractTimestamp(ForceTable forceTable, long l) {
                        SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return timeFormatter.parse(forceTable.getTime_stamp()).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    public static RichSinkFunction<ForceTable> JdbcWriter(String dbName) {
        return new RichSinkFunction<ForceTable>() {
            private transient Connection connection;
            private transient PreparedStatement statement;

            @Override
            public void open(Configuration parameters) throws Exception {
                String connectionStr = "jdbc:mysql://" + host + ":3306/" + dbName + "?useSSL=false";
                connection = DriverManager.getConnection(connectionStr, user, password);
            }

            @Override
            public void invoke(ForceTable forceTable, Context context) throws Exception {
                String sql = "INSERT INTO forcertable (abutmentno, bridgeid, pierno, forcevalue, time_stamp) VALUES (?, ?, ?, ?, ?)";
                try {
                    if (statement == null) {
                        statement = connection.prepareStatement(sql);
                    }
                    statement.setString(1, forceTable.getAbutmentno());
                    statement.setString(2, forceTable.getBridgeid());
                    statement.setString(3, forceTable.getPierno());
                    statement.setFloat(3, forceTable.getForcevalue());
                    statement.setString(4, forceTable.getTime_stamp()); // 注意：这里应该是一个 Timestamp 对象，而不是 String
                    statement.executeUpdate();
                } catch (SQLException e) {
                    // 处理异常，打印错误信息或进行其他错误处理
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        };
    }

}
