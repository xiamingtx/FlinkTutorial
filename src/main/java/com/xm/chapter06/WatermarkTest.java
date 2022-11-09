package com.xm.chapter06;

import com.xm.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author 夏明
 * @version 1.0
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100); // 默认200ms 每个interval触发一次水位线生成

        // 从元素读取数据
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                        new Event("Mary", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./prod?id=100", 3000L),
                        new Event("Bob", "./prod?id=1", 3300L),
                        new Event("Alice", "./prod?id=200", 3200L),
                        new Event("Bob", "./home", 3500L),
                        new Event("Bob", "./prod?id=2", 3800L),
                        new Event("Bob", "./prod?id=3", 4200L))
                // 有序流的Watermark生成
                // .assignTimestampsAndWatermarks(WatermarkStrategy
                //         .<Event>forMonotonousTimestamps()
                //         .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                //             @Override
                //             public long extractTimestamp(Event element, long recordTimestamp) {
                //                 return element.timestamp * 1000L;
                //             }
                //         }));
                // 乱序流的Watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp * 1000L;
                            }
                        }));

        env.execute();
    }
}
