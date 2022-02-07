package org.example.flink.apitest.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.apitest.beans.SensorReading;
import org.example.flink.apitest.source.SourceTest2_File;

public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //从文件读取数据
        String inputPath = TransformTest2_RollingAggregation.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //转换为SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());

        DataStream<Long> longDataStream = env.fromElements(1L, 34L, 4L, 657L, 23L);
        KeyedStream<Long, Integer> longIntegerKeyedStream = longDataStream.keyBy(new KeySelector<Long, Integer>() {
            @Override
            public Integer getKey(Long value) throws Exception {
                return value.intValue() % 2;
            }
        });

        //KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(SensorReading::getId);

        //滚动聚合，取当前最大的温度值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print("result");
        //keyedStream1.print("key1");
        //longIntegerKeyedStream.sum(0).print("key2");

        //执行
        env.execute();
    }
}
