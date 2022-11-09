package com.xm.chapter06;

import com.xm.chapter05.ClickSource;
import com.xm.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author 夏明
 * @version 1.0
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100); // 默认200ms 每个interval触发一次水位线生成

        // SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("mytencent", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] fields = value.split(" ");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }
                        )
                );

        stream.print("input");

        // 定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late"){}; // 使用匿名内部类 否则会被类型擦除

        // 统计每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) // 允许一分钟延迟
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");
        result.getSideOutput(late).print("late");

        env.execute();
    }
}
