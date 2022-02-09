package org.example.flink.apitest.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.flink.apitest.beans.SensorReading;

import java.time.Duration;

/**
 * @Description: 事件时间窗口
 *
 * https://blog.csdn.net/love20991314/article/details/121482687
 *
 * @Author: hekai
 * @Date: 2022-02-09 14:53
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1); //设置并行度

        // In Flink 1.12 the default stream time characteristic has been changed to TimeCharacteristic.EventTime,
        // thus you don't need to call this method for enabling event-time support anymore.
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置事件时间

        env.getConfig().setAutoWatermarkInterval(100); //设置周期生成WaterMark间隔，100ms

        //socket文本流读取测试数据
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //将读入的数据转换成SensorReading类型，分配时间戳和watermark
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })

                //升序数据设置事件时间和watermark
                // This method uses the deprecated watermark generator interfaces.
                // Please switch to assignTimestampsAndWatermarks(WatermarkStrategy) to use the new interfaces instead.
                // The new interfaces support watermark idleness and no longer need to differentiate between "periodic"
                // and "punctuated" watermarks.
                //.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                //    @Override
                //    public long extractAscendingTimestamp(SensorReading element) {
                //        return element.getTimestamp() * 1000L;
                //    }
                //})

                //乱序数据 设置时间戳和watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //指定watermark生成策略，最大延迟长度2s
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            //SerializableTimestampAssigner接口中实现了extractTimestamp方法来指定如何从事件数据中抽取时间戳
                                                   @Override
                                                   public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                                       return element.getTimestamp() * 1000L;
                                                   }
                                               }
                        )
                )
                ;

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};
        //基于事件时间的开窗聚合，统计15s内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();


    }
}
