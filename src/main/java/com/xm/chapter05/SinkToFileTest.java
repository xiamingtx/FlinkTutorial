package com.xm.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author 夏明
 * @version 1.0
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Alice", "./prod?id=200", 3200L),
                new Event("Bob", "./home", 3500L),
                new Event("Bob", "./prod?id=2", 3800L),
                new Event("Bob", "./prod?id=3", 4200L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8")).withRollingPolicy(
                DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024) // 1G 滚动文件
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 15分钟滚动一个文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 五分钟不来信息就归档
                        .build()
        ).build();
        stream.map(Event::toString).addSink(streamingFileSink);

        env.execute();
    }
}
