package com.xm.chapter06;

import com.xm.chapter05.ClickSource;
import com.xm.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author 夏明
 * @version 1.0
 */
// 统计pv和uv 两者相除得到平均用户活跃度
public class WindowAggregateTest_PvUv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100); // 默认200ms 每个interval触发一次水位线生成

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("data");
        // 所有数据放在一起统计pv和uv
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();

        env.execute();
    }

    // 自定义一个AggregateFunction 用Long保存pv个数 用HashSet做uv去重
    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
            // 每来一条数据 pv个数加一 将user放入HashSet中去重
            accumulator.f1.add(value.user);
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            // 窗口触发时 输出pv和uv的比值
            return (double) accumulator.f0 / accumulator.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
            return null;
        }
    }
}
