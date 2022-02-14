package org.example.flink.apitest.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.flink.apitest.beans.SensorReading;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description: 需要注释 flink-table-planner-blink_2.12 1.13.5，否则会报错：
 * ClassNotFoundException: org.apache.flink.shaded.guava18.com.google.common.collect.Sets
 *
 * @Author: hekai
 * @Date: 2022-02-14 16:33
 */
public class TableTest1_Example {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.读取数据
        String inputPath = TableTest1_Example.class.getClassLoader().getResource("sensor.txt").getPath();
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //2.转为pojo
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //3.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        //5.调用table api进行转换操作
        Table resultTable = dataTable.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        //6.执行sql
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toDataStream(resultTable, Row.class).print("result");
        tableEnv.toDataStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
