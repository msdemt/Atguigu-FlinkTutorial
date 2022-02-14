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
 * @Date: 2022-02-14 9:09
 */
public class ProcessTest2_ApplicationCase {

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
                .process(new TempConsIncreWarning(10))
                .print();

        env.execute();
    }

    //实现自定义处理函数，检测一段时间内的温度连续上升，输出报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {
        //定义私有属性，当前统计的时间间隔
        private Integer interval;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        //定义状态，保存上一次的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;


        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            //取出状态
            Double lastTemp = lastTempState.value() == null ? Double.MIN_VALUE : lastTempState.value();
            Long timerTs = timerTsState.value();

            //如果温度上升并且没有定时器，注册10s后的定时器，开始等待
            if(value.getTemperature() > lastTemp && timerTs == null){
                //计算定时器时间戳
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            else if(value.getTemperature() < lastTemp && timerTs != null){
                //如果温度下降，那么删除定时器
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
