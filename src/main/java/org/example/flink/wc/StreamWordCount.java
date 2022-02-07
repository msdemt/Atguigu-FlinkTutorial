package org.example.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理wordcount
 * 开启socket监听：nc -lk 7777
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用代码配置并行度
        //env.setParallelism(1);
        //禁止相同并行度的on to one操作的算子链接成一个task
        //env.disableOperatorChaining();

        //用ParameterTool工具从程序启动参数中提取配置项
        //program argument中添加 --host localhost --port 7777
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        //从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        //基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new WordCount.MyFlatMapper())
                .keyBy(tuple2 -> tuple2.getField(0))
                .sum(1)
                .setParallelism(2) //配置并行度
                .slotSharingGroup("red"); //并行分组
        wordCountDataStream.print().setParallelism(1); //配置输出的并行度

        //执行任务
        env.execute();
    }
}
