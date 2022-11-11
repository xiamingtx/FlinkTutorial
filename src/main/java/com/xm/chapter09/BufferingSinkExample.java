package com.xm.chapter09;

import com.xm.chapter05.ClickSource;
import com.xm.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * @author 夏明
 * @version 1.0
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置检查点
        env.enableCheckpointing(1000L); // 每隔1s设置检查点
        // env.setStateBackend(new HashMapStateBackend()); // 默认
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(60000L); // 超时时间设置
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 精确一次
        // checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE); // 至少一次
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableUnalignedCheckpoints(); // 开启非对齐 必须是精确一次
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 开启外部持久化 在任务取消后保留检查点
        checkpointConfig.setTolerableCheckpointFailureNumber(0); // 失败多少次是可以允许的

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    // 自定义实现SinkFunction
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义当前类的属性 批量
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        private List<Event> bufferedElements;

        // 定义一个算子状态
        private ListState<Event> checkpointedState; // 考虑故障恢复 采用checkpoint机制

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value); // 缓存到列表
            // 判断如果达到阈值就批量写入
            if (bufferedElements.size() == threshold) {
                // 用打印到控制台模拟写入外部系统
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }

                System.out.println("===============输出完毕===============");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态
            checkpointedState.clear();

            // 对状态进行持久化, 复制缓存的列表到列表状态
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果从故障恢复 需要将ListState中所有的元素放入本地变量列表中去
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }
    }
}
