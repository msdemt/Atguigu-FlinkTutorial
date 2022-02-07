package org.example.flink.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.flink.apitest.beans.SensorReading;

/**
 * split和select api 从flink 1.12开始就不支持了。
 * https://nightlies.apache.org/flink/flink-docs-release-1.11/dev/stream/operators/
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/stream/operators/
 *
 * Split...Select...已经过时，推荐使用更灵活的侧路输出(Side-Output)
 * https://blog.csdn.net/wangpei1949/article/details/99698868
 * https://blog.csdn.net/wudonglianga/article/details/121594858
 *
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        String inputPath = TransformTest4_MultipleStreams.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //split分流，按照温度30为界分为两个流
        //Split...Select...中Split只是对流中的数据打上标记,并没有将流真正拆分。可通过Select算子将流真正拆分出来。
        //Split...Select...不能连续分流。即不能Split...Select...Split，但可以如Split...Select...Filter...Split。
        //Split...Select...已经过时，推荐使用更灵活的侧路输出(Side-Output)
        /* SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelecotr<SensorReading>(){
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all"); */


        //使用sideoutput 侧路输出 进行分流
        //Side-Output是从Flink 1.3.0开始提供的功能，支持了更灵活的多路输出。
        //Side-Output可以以侧流的形式，以不同于主流的数据类型，向下游输出指定条件的数据、异常数据、迟到数据等等。
        //Side-Output通过ProcessFunction将数据发送到侧路OutputTag

        //定义一个OutputTag，用来表示侧路输出低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("low"){};
        //定义侧路输出实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                //判断温度，大于30度，高温流输出到主流，小于低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTempTag, value);
                }
            }
        });
        highTempStream.print("high");
        highTempStream.getSideOutput(lowTempTag).print("low");

        DataStream lowTempStream = highTempStream.getSideOutput(lowTempTag);

        //合流 Union可以将两个或多个同数据类型的流合并成一个流
        //highTempStream和lowTempStream的数据类型是相同的，可以使用union合流
        highTempStream.union(lowTempStream).print("union");


        //Connect可以用来合并两种不同类型的流。
        //Connect合并后，可用map中的CoMapFunction或flatMap中的CoFlatMapFunction来对合并流中的每个流进行处理。
        //将高温流转换成二元组，然后与低温流合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warningStream.connect(lowTempStream);

        SingleOutputStreamOperator<Object> resultStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("connect-map");

        //执行
        env.execute();
    }
}
