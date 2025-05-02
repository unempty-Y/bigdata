package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class Watermarks {
    public WatermarkStrategy<OrderMaster> orderMasterWatermarkStrategy(){
        return WatermarkStrategy.<OrderMaster>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderMaster>() {
                    @Override
                    public long extractTimestamp(OrderMaster orderMaster, long l) {
                        SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
                        try {
                            return timeFormatter.parse(orderMaster.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
    public WatermarkStrategy<ProduceRecord> produceRecordWatermarkStrategy(){
        return WatermarkStrategy.<ProduceRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProduceRecord>() {
                    @Override
                    public long extractTimestamp(ProduceRecord produceRecord, long l) {
                        SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return timeFormatter.parse(produceRecord.getProduceCodeStartTime()).getTime();
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
}
