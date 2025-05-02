package org.example.gs7.shili;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class FlinkJoinExample {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 初始化用户数据流
// 初始化用户数据流并分配时间戳和水印
        DataStream<User> userStream = env.fromElements(
                new User("user1", "Alice", 1617180000L),
                new User("user2", "Bob", 1617180020L),
                new User("user3", "Charlie", 1617180040L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<User>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(User element) {
                // 假设User类有一个timestamp字段
                return element.date;
            }
        });

// ... 其余代码保持不变 ...


        // 初始化用户行为数据流
        DataStream<UserAction> userActionStream = env.fromElements(
                new UserAction("user1", "click", 1617180000L),
                new UserAction("user2", "view", 1617180020L),
                new UserAction("user1", "click", 1617180040L)
        );

        // 为用户行为数据流分配时间戳和水印
        DataStream<UserAction> userActionStreamWithTimestamps = userActionStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserAction>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(UserAction element) {
                        return element.timestamp;
                    }
                });

        // 根据用户ID进行连接操作
        DataStream<Tuple2<User, UserAction>> joinedStream = userStream
                .join(userActionStreamWithTimestamps)
                .where(user -> user.userId)//定义user流的key
                .equalTo(userAction -> userAction.userId)//定义userAction的key
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))//窗口
                .apply(new JoinFunction<User, UserAction, Tuple2<User, UserAction>>() {
                    @Override
                    public Tuple2<User, UserAction> join(User user, UserAction userAction) {
                        return new Tuple2<>(user, userAction);//连接逻辑
                    }
                });

        // 打印结果
        joinedStream.print();

        // 执行程序
        env.execute("Flink Join Example");
    }
}
