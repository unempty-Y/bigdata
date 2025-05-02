package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class JdbcWriter implements Constants, Serializable {
    private Connection connection;
    private Statement statement;
    private String user;
    private String password;
    private int port;

    /**
     * @param dbType 数据库类型mysql或clickhouse
     * @param dbName 数据库名
     * @param tbName 表名
     * @return
     * @throws
     * @input 支持任意类型cassClass值
     * @version 2024/6/7 15:08
     */

    public RichSinkFunction<Object> JdbcWriter(String dbType, String dbName, String tbName) {
        if (dbType.equals("mysql")) {
            user = mysqlUser_Password[0];
            password = mysqlUser_Password[1];
            port = mysqlPort;
        } else {
            user = clickhouseUser_Password[0];
            password = clickhouseUser_Password[1];
            port = clickhousePort;
        }

        return new RichSinkFunction<Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                String connectionStr = "jdbc:" + dbType + "://" + bigdata1 + ":" + port + "/" + dbName;
                if (dbType.equals("mysql")) {
                    connectionStr = connectionStr + "?useSSL=false";
                }
                connection = DriverManager.getConnection(connectionStr, mysqlUser_Password[0], mysqlUser_Password[1]);
                statement = connection.createStatement();
            }

            @Override
            public void invoke(Object value, Context context) throws Exception {
                Field[] fields = value.getClass().getDeclaredFields();
                StringBuilder sb = new StringBuilder("(");
                for (Field field : fields) {
                    field.setAccessible(true);
                    if (sb.length() > 1) sb.append(",");
                    if (field.get(value) instanceof String) {
                        sb.append("'").append(field.get(value)).append("'");
                    } else {
                        sb.append(field.get(value));
                    }
                }
                sb.append(")");
                String sql = "insert into " + tbName + " values " + sb.toString();
                statement.execute(sql);
            }

            @Override
            public void close() throws Exception {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            }
        };
    }
}
