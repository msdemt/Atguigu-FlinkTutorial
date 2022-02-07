package org.example.flink.apitest.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.flink.apitest.beans.SensorReading;

/**
 * 输出到redis
 * 对独立部署、集群部署、sentinel部署的redis使用的方式不通
 * 详见：https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 *
 * 内置jedis版本是2.8.0，对接redis 4.0以上的较新版本，会出现解析cluster node 地址host和port信息出错的问题
 * 详见：https://blog.csdn.net/xiaohu21/article/details/113486585
 *
 * 不推荐使用redis，当前版本flink-connector-redis_2.11 1.0 还不支持redis过期时间
 */
public class SinkTest2_Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        String inputPath = SinkTest2_Redis.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStreamSource<String> dataStreamSource = env.readTextFile(inputPath);

        //转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataSource = dataStreamSource.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义连接单点redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();

        dataSource.addSink(new RedisSink<>(conf, new MyRedisMapper()));

        //执行
        env.execute();

    }
    //自定义RedisMapper
    private static class MyRedisMapper implements RedisMapper<SensorReading> {
        //定义保存数据到redis的命令，存成hash表，hset sensor_temp id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }
}
