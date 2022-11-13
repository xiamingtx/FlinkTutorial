package com.xm.chapter11;

import com.xm.chapter05.ClickSource;
import com.xm.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 夏明
 * @version 1.0
 */
public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据 得到DataStream
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 2. 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. 将DataStream转换为表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        // 4. 直接写SQL进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user, url, `timestamp` from " + eventTable);

        // 5. 基于Table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // 6. 转换成流打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");

        // 7. 聚合转换
        tableEnv.createTemporaryView("clickTable", eventTable);
        Table aggResult = tableEnv.sqlQuery("select user, COUNT(url) as cnt from clickTable group by user");

        tableEnv.toChangelogStream(aggResult).print("agg");

        env.execute();
    }
}
