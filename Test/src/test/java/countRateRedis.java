import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.*;
import org.example.KafkaConnect;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;


public class countRateRedis {
    public static void main(String[] args) throws Exception {
        String topic = "forceTable";

        OutputTag<ForceTable> overloadtag = new OutputTag<ForceTable>("overloadtag") {
        };
        OutputTag<ForceTable> voidwarningstag = new OutputTag<ForceTable>("voidwarningstag") {
        };
        OutputTag<ForceTable> voidstag = new OutputTag<ForceTable>("voidstag") {
        };
        KafkaSource<String> source = new KafkaConnect().kafkaReader(topic);
        ProcessFunction<ForceTable, ForceTable> processFunction = new ProcessFunction<ForceTable, ForceTable>() {
            @Override
            public void processElement(ForceTable forceTable, ProcessFunction<ForceTable, ForceTable>.Context context, Collector<ForceTable> collector) throws Exception {
                collector.collect(forceTable);
                if (forceTable.getForcevalue() > 500) context.output(overloadtag, forceTable);
                else if (forceTable.getForcevalue() == 0) context.output(voidstag, forceTable);
                else if (forceTable.getForcevalue() < 30) context.output(voidwarningstag, forceTable);
            }
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<ForceTable> forceTableSingleOutputStreamOperator = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), topic)
                .map(i -> {
                    Gson gson = new GsonBuilder().create();
                    return gson.fromJson(i, ForceTable.class);
                })
                .filter(i -> i.forcevalue >= 0)//初步清洗
                .assignTimestampsAndWatermarks(forceTableWatermarkStrategy());

        SingleOutputStreamOperator<ForceTable> dwdData = forceTableSingleOutputStreamOperator
                .keyBy(ForceTable::getBridgeid)
                .process(processFunction);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> allStatusCounts = dwdData
                .map(new MapFunction<ForceTable, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> map(ForceTable forceTable) throws Exception {
                        return new Tuple3<>("allStatusCount", forceTable.bridgeid, 1.0);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(2);
        SingleOutputStreamOperator<Tuple3<String, String,Double>> overloadtagCounts = dwdData
                .getSideOutput(overloadtag)
                .map(new MapFunction<ForceTable, Tuple3<String, String,Double>>() {
                    @Override
                    public Tuple3<String, String,Double> map(ForceTable forceTable) throws Exception {
                        return new Tuple3<>("overloadtagcount",forceTable.bridgeid, 1.0);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(2);
        SingleOutputStreamOperator<Tuple3<String, String,Double>> voidwarningCounts = dwdData
                .getSideOutput(voidwarningstag)
                .map(new MapFunction<ForceTable, Tuple3<String, String,Double>>() {
                    @Override
                    public Tuple3<String, String,Double> map(ForceTable forceTable) throws Exception {
                        return new Tuple3<>("voidwarningCounts",forceTable.bridgeid, 1.0);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(2);
        SingleOutputStreamOperator<Tuple3<String, String,Double>> voidCounts = dwdData
                .getSideOutput(voidstag)
                .map(new MapFunction<ForceTable, Tuple3<String, String,Double>>() {
                    @Override
                    public Tuple3<String, String,Double> map(ForceTable forceTable) throws Exception {
                        return new Tuple3<>("voidCounts",forceTable.bridgeid, 1.0);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum(2);

        DataStream<Tuple2<String, String>> voidwarningRate = allStatusCounts
                .join(voidwarningCounts)
                .where(i -> true)
                .equalTo(i -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply((voidwarningCount, allstatusCount) -> {
                    double rate = ((voidwarningCount.f2 * 1.0) / (allstatusCount.f2 * 1.0));
                    String s = String.format("%.1f", rate);
                    return new Tuple2<>(voidwarningCount.f1, s+"%");
                });
        DataStream<Tuple2<String, String>> overloadRate = allStatusCounts
                .join(overloadtagCounts)
                .where(i -> true)
                .equalTo(i -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply((overloadtagCount, allstatusCount) -> {
                    double rate = ((overloadtagCount.f2 * 1.0) / (allstatusCount.f2 * 1.0));
                    String s = String.format("%.1f", rate);
                    return new Tuple2<>(overloadtagCount.f1, s+"%");
                });
        DataStream<Tuple2<String, String>> voidRate = allStatusCounts
                .join(voidCounts)
                .where(i -> true)
                .equalTo(i -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply((voidCount, allstatusCount) -> {
                    double rate = ((voidCount.f2 * 1.0) / (allstatusCount.f2 * 1.0));
                    String s = String.format("%.1f", rate);
                    return new Tuple2<>(voidCount.f1, s + "%");
                });
        voidwarningRate.addSink(new RedisWriter().redisHSet("voidwarningRate"));
        overloadRate.addSink(new RedisWriter().redisHSet("overloadRate"));
        voidRate.addSink(new RedisWriter().redisHSet("voidRate"));
        voidwarningRate.map(new MapFunction<Tuple2<String, String>, Object>() {
            @Override
            public Object map(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new RateTable("脱空预警率",stringStringTuple2.f0, stringStringTuple2.f1);
            }
        }).addSink(new JdbcWriter().JdbcWriter("mysql", "fanlidatabase", "ratetable"));
        overloadRate.map(new MapFunction<Tuple2<String, String>, Object>() {
            @Override
            public Object map(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new RateTable("超载率",stringStringTuple2.f0, stringStringTuple2.f1);
            }
        }).addSink(new JdbcWriter().JdbcWriter("mysql", "fanlidatabase", "ratetable"));
        voidRate.map(new MapFunction<Tuple2<String, String>, Object>() {
            @Override
            public Object map(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new RateTable("脱空率",stringStringTuple2.f0, stringStringTuple2.f1);
            }
        }).addSink(new JdbcWriter().JdbcWriter("mysql", "fanlidatabase", "ratetable"));
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

    public static class RateTable {
        String bridgeid;
        String name;
        String rate;

        public RateTable(String bridgeid, String name, String rate) {
            this.bridgeid = bridgeid;
            this.name = name;
            this.rate = rate;
        }
    }

    public static class ForceTable {
        private String abutmentno;
        private String bridgeid;
        private String pierno;
        private float forcevalue;
        private String time_stamp;

        public ForceTable(String abutmentno, String bridgeid, String pierno, float forcevalue, String time_stamp) {
            this.abutmentno = abutmentno;
            this.bridgeid = bridgeid;
            this.pierno = pierno;
            this.forcevalue = forcevalue;
            this.time_stamp = time_stamp;
        }

        public String getAbutmentno() {
            return abutmentno;
        }

        public void setAbutmentno(String abutmentno) {
            this.abutmentno = abutmentno;
        }

        public String getBridgeid() {
            return bridgeid;
        }

        public void setBridgeid(String bridgeid) {
            this.bridgeid = bridgeid;
        }

        public String getPierno() {
            return pierno;
        }

        public void setPierno(String pierno) {
            this.pierno = pierno;
        }

        public float getForcevalue() {
            return forcevalue;
        }

        public void setForcevalue(float forcevalue) {
            this.forcevalue = forcevalue;
        }

        public String getTime_stamp() {
            return time_stamp;
        }

        public void setTime_stamp(String time_stamp) {
            this.time_stamp = time_stamp;
        }
    }

}
