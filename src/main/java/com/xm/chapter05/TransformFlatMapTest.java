package com.xm.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 夏明
 * @version 1.0
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. 实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");

        // 2. 传入一个lambda表达式
        stream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Mary")) {
                out.collect(value.url);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {}).print("2");

        env.execute();
    }

    // 实现一个自定义的FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
