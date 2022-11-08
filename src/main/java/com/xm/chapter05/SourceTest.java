package com.xm.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author 夏明
 * @version 1.0
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4. 从socket文本流中读取
        DataStreamSource<String> stream4 = env.socketTextStream("mytencent", 7777);

        // stream1.print("1");
        // numStream.print("nums");
        // stream2.print("2");
        // stream3.print("3");
        // stream4.print("4");

        // 5. 从kafka中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "mytencent:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream =
                env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        kafkaStream.print();

        env.execute();
    }
}
