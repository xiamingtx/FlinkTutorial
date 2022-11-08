package com.xm.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author 夏明
 * @version 1.0
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 创建一个jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("mytencent").build();

        // 写入redis
        stream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    // 自定义类实现RedisMapper接口
    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }
    }
}
