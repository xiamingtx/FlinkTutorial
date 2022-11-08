package com.xm.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 夏明
 * @version 1.0
 */
public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

        // 1. 传入一个实现了FilterFunction的类的对象
        SingleOutputStreamOperator<Event> res1 = stream.filter(new MyFilter());

        // 2. 传入一个匿名类实现FilterFunction
        SingleOutputStreamOperator<Event> res2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });

        // 3. 传入lambda表达式
        stream.filter(data -> data.user.equals("Alice")).print("lambda: Alice click");

        res1.print();
        res2.print();

        env.execute();
    }

    // 实现一个自定义的FilterFunction
    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Mary");
        }
    }
}
