package org.example.flink.apitest.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.apitest.beans.SensorReading;

/**
 * @Description:
 * @Author: hekai
 * @Date: 2022-02-14 10:29
 */
public class StateTest2_keyedState {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转换成SensorReading类型
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个有状态的map操作，统计当前sensor数据个数
        dataStream.keyBy(SensorReading::getId)
                .map(new MyKeyCountMapper());


    }

    //自定义RichFunction
    private static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> keyCountState;

        //其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            //myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>());
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            //其他状态API调用
            //list state
            for(String str : myListState.get()){
                System.out.println(str);
            }
            myListState.add("hello");

            //map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");

            //reducing state
            //myReducingState.add(value);

            myMapState.clear();

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
