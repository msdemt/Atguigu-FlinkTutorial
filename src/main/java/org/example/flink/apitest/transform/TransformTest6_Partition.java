package org.example.flink.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.apitest.beans.SensorReading;

public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //从文件读取数据
        String inputPath = TransformTest6_Partition.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStreamSource<String> inputStream = env.readTextFile(inputPath);

        //转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("input");

        //1.shuffle 洗牌
        DataStream<String> shuffledStream = inputStream.shuffle();
        shuffledStream.print("shuffle");

        //2.keyBy 分组
        dataStream.keyBy(data -> data.getId()).print("keyBy");

        //3.global 将所有数据使用第一个operator实例输出
        dataStream.global().print("global");

        //执行
        env.execute();

    }
}
