package com.xm.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author 夏明
 * @version 1.0
 */
public class UdfTest_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 2. 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);

        // 3. 调用UDF 进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user, WeightedAverage(ts, 1) as w_avg " +
                "from clickTable group by user");

        // 4. 转换成流打印输出
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }

    // 单独定义一个累加器类型
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // 实现自定义的聚合函数 计算加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加计算的方法
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;
            accumulator.count += iWeight;
        }
    }
}
