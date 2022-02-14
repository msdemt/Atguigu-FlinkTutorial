package org.example.flink.apitest.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.apitest.beans.SensorReading;

/**
 * @Description:
 * @Author: hekai
 * @Date: 2022-02-14 8:40
 */
public class ProcessTest1_keyedProcessFunction {

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

        //测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy(SensorReading::getId)
                .process(new MyProcess())
                .print();

        env.execute();
    }

    //实现自定义的处理函数
    private static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {

        ValueState<Long> tsTimerState;

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());
            ctx.timestamp();
            ctx.getCurrentKey();
            //ctx.output();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);

            //ctx.timerService().registerEventTimeTimer((value.getTimestamp()+10)*1000L);
            //ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
            //ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
