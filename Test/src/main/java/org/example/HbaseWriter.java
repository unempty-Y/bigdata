package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.Serializable;


public class HbaseWriter implements Constants, Serializable {
    private Connection connection;
    private Table table;


    /**
     * 创建一个富SinkFunction，用于将数据写入指定的HBase表。
     *
     * @param tbName 要写入的HBase表名
     * @return 实现了RichSinkFunction接口的SinkFunction
     * @input rowKey, (family, list, value)
     */
    public RichSinkFunction<Tuple2<String, Tuple3<String, String, String>[]>> hbaseWriter(String tbName) {
        return new RichSinkFunction<Tuple2<String, Tuple3<String, String, String>[]>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set(HConstants.ZOOKEEPER_QUORUM, bigdata1);
                config.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort + "");
                connection = ConnectionFactory.createConnection(config);
                table = connection.getTable(TableName.valueOf(tbName));
            }

            @Override
            public void close() throws Exception {
                table.close();
                connection.close();
            }

            @Override
            public void invoke(Tuple2<String, Tuple3<String, String, String>[]> value, Context context) throws Exception {
                Put put = new Put(value.f0.getBytes());
                for (Tuple3<String, String, String> i : value.f1) {
                    put.addColumn(i.f0.getBytes(), i.f1.getBytes(), i.f2.getBytes());
                }
                table.put(put);
            }
        };
    }
}
