package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Demo9_mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.BATCH);
        en.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> source = en.readTextFile("src/main/resources/socres").filter(x -> !x.split(",")[0].equals("id"))
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        Integer key = Integer.valueOf(s.split(",")[0]);
                        Integer sum = 0;
                        for (String one : s.split(",")[2].split("\\|")) {
                            sum = sum + Integer.valueOf(one);
                        }


                        return Tuple2.of(key, sum);
                    }
                });

source.addSink(new RichSinkFunction<Tuple2<Integer, Integer>>() {
    Connection conn;
    PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
         conn = DriverManager.getConnection("jdbc:mysql://192.168.45.16:3306/wh", "root", "123456");
         preparedStatement = conn.prepareStatement("insert into  student(id,sum) values (?,?)");

    }



    @Override
    public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
        super.invoke(value, context);
        preparedStatement.setInt(1,value.f0);
        preparedStatement.setInt(2,value.f1);
        preparedStatement.execute();


    }

    @Override
    public void close() throws Exception {
        super.close();
        preparedStatement.close();
        conn.close();
    }
});



        en.execute("aa");
    }
}
