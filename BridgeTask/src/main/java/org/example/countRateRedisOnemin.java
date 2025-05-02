package org.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;


public class countRateRedisOnemin {
    private static final String host =
            "192.168.31.128";
    //            "127.0.0.1"; // 目标服务器IP地址
    private static final int server_port = 40126; // 目标服务器端口号
    //mysql账号
    private static final String user = "root";
    private static final String password = "123456";
    private static final String databases = "fanlidatabase";
    public static class RatePercentageWindowFunction implements WindowFunction<OneMinRateTable, OneMinRateTable, Tuple2<String,String>, TimeWindow> {

        @Override
        public void apply(Tuple2<String,String> tuple, TimeWindow window, Iterable<OneMinRateTable> input, Collector<OneMinRateTable> out) throws Exception {
            // 初始化计数器
            double allCount = 0;
            double overloadCount = 0;
            double voidCount = 0;

            // 计算每个状态的数量
            for (OneMinRateTable oneMinRateTable : input) {
                if (oneMinRateTable.getOverloadrate() == 0.0) {
                    allCount += oneMinRateTable.getVoidrate();
                } else if (oneMinRateTable.getOverloadrate() == 1.0) {
                    overloadCount += oneMinRateTable.getVoidrate();
                } else if (oneMinRateTable.getOverloadrate() == 2.0) {
                    voidCount += oneMinRateTable.getVoidrate();
                }
            }

            // 计算百分比并输出
            if (allCount > 0) {
                System.out.println(overloadCount+","+allCount+","+voidCount);
                double overloadPercentage = (overloadCount / allCount) * 100;
                double voidPercentage = (voidCount / allCount) * 100;
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                out.collect(new OneMinRateTable(tuple._1,tuple._2 , overloadPercentage,voidPercentage, dateFormat.format(window.getEnd())));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        String topic = "ada";

        OutputTag<ForceTable> warningstag = new OutputTag<ForceTable>("warningstag") {
        };
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic,host+":"+9092);
        ProcessFunction<ForceTable, OneMinRateTable> processFunction = new ProcessFunction<ForceTable, OneMinRateTable>() {
            @Override
            public void processElement(ForceTable forceTable, ProcessFunction<ForceTable, OneMinRateTable>.Context context, Collector<OneMinRateTable> collector) throws Exception {
                collector.collect(new OneMinRateTable(forceTable.getBridgeid(),forceTable.getAbutmentno(),0.0,1.0,forceTable.getTime_stamp()));
                if (forceTable.getForcevalue() > 500) {
                    collector.collect(new OneMinRateTable(forceTable.getBridgeid(),forceTable.getAbutmentno(),1.0,1.0,forceTable.getTime_stamp()));
                    context.output(warningstag, forceTable);
                }
                else if (forceTable.getForcevalue() < 30) {
                    collector.collect(new OneMinRateTable(forceTable.getBridgeid(),forceTable.getAbutmentno(),2.0,1.0,forceTable.getTime_stamp()));
                    context.output(warningstag, forceTable);
                }
            }
        };

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

        // 读取数据
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

        SingleOutputStreamOperator<OneMinRateTable> dwdData = watermakerSource
                .keyBy(ForceTable::getBridgeid)
                .process(processFunction);

        SingleOutputStreamOperator<OneMinRateTable> allStatusCounts = dwdData
                .keyBy(new KeySelector<OneMinRateTable, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> getKey(OneMinRateTable oneMinRateTable) throws Exception {
                        return new Tuple2<>(oneMinRateTable.getBridgeid(),oneMinRateTable.getAbutmentno());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new RatePercentageWindowFunction());

allStatusCounts.print();
//        allStatusCounts.filter(i->i.getRatename().equals("overloadRate"))
//                .map(i->new Tuple2<String,String>(i.getBridgeid(),String.format("%.2f",i.getRatevalue())))
//                .addSink(new RedisWriter().redisHSet("overloadRate"));
//        allStatusCounts.filter(i->i.getRatename().equals("voidRate"))
//                .map(i->new Tuple2<String,String>(i.getBridgeid(),String.format("%.2f",i.getRatevalue())))
//                .addSink(new RedisWriter().redisHSet("voidRate"));
        allStatusCounts.addSink(JdbcWriter("fanlidatabase"));
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

    static Statement statement;
    static Connection connection;

    public static RichSinkFunction<OneMinRateTable> JdbcWriter(String dbName) {
        return new RichSinkFunction<OneMinRateTable>() {
            private transient Connection connection;
            private transient PreparedStatement statement;

            @Override
            public void open(Configuration parameters) throws Exception {
                String connectionStr = "jdbc:mysql://" + host + ":3306/" + dbName + "?useSSL=false";
                connection = DriverManager.getConnection(connectionStr, user, password);
            }

            @Override
            public void invoke(OneMinRateTable oneMinRateTable, Context context) throws Exception {
                String sql = "INSERT INTO oneminratetable (bridgeid,abutmentno, overloadrate, voidrate, time_stamp) VALUES ( ?, ?, ?, ?, ? )";
                try {
                    if (statement == null) {
                        statement = connection.prepareStatement(sql);
                    }
                    statement.setString(1, oneMinRateTable.getBridgeid());
                    statement.setString(2, oneMinRateTable.getAbutmentno());
                    statement.setFloat(3, oneMinRateTable.getOverloadrate().floatValue());
                    statement.setFloat(4, oneMinRateTable.getVoidrate().floatValue());
                    statement.setString(5, oneMinRateTable.getTime_stamp());
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
