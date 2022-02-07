package org.example.flink.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        String inputPath = SourceTest2_File.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStream<String> dataStream = env.readTextFile(inputPath);

        //打印输出
        dataStream.print();

        //执行
        env.execute();
    }
}
