package org.example;

public interface Constants {
//      String bigdata1 = "192.168.45.16";
//      String bigdata2 = "192.168.45.22";
//      String bigdata3 = "192.168.45.23";
    String bigdata1 = "192.168.31.128";
    String bigdata2 = "bigdata2";
    String bigdata3 = "bigdata3";
    int kafkaPort = 9092;
    int zookeeperPort=2181;
    int redisPort = 6379;
    int mysqlPort = 3306;
    String[] mysqlUser_Password = {"root", "123456"};
    int clickhousePort = 8123;
    String[] clickhouseUser_Password = {"default", "123456"};
    int hudiPort = 9083;
    String[] filter = {"已签收","已付款"}; // "已发货", "已付款", "已下单","已签收","已退款"

}
