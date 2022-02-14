package org.example.flink.apitest.processfunction;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.apitest.beans.SensorReading;

/**
 * @Description:
 * @Author: hekai
 * @Date: 2022-02-14 9:40
 */
public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个OutputTag，用来表示侧输出流低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){};

        //测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                //判断温度，大于30度，高温流输出到主流，小于30度，低温流输出到侧输出流
                if(value.getTemperature() > 30){
                    out.collect(value);
                }else{
                    ctx.output(lowTempTag, value);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }
}
