package org.example.flink.apitest.window;


import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.apitest.beans.SensorReading;

/**
 * @Description: window test
 * @Author: hekai
 * @Date: 2022-02-09 9:53
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //开窗测试

        //1.增量聚合函数
        SingleOutputStreamOperator<Integer> resultStream1 = dataStream.keyBy(SensorReading::getId)
                //.countWindow(10,2);
                //.window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        //2.全窗口函数
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        });
        WindowedStream<SensorReading, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(15))); //滚动处理时间窗口
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = windowedStream.apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                String id = s;
                Long windowEnd = window.getEnd();
                Integer count = IteratorUtils.toList(input.iterator()).size();
                out.collect(new Tuple3<>(id, windowEnd, count));
            }
        });


        //3.其他可选API
        //定义一个OutputTag，用来表示侧路输出
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                //.trigger()
                //.evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");
        sumStream.getSideOutput(outputTag).print("late");


        resultStream2.print();
        env.execute();


    }
}
