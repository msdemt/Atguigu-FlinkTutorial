package org.example.flink.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description:
 * @Author: hekai
 * @Date: 2022-02-14 17:23
 */
public class TableTest2_CommonApi {

    public static void main(String[] args) {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1基于老版本planner的流处理
        //EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
        //        .useOldPlanner() //The old planner has been removed in Flink 1.14.
        //        .inStreamingMode()
        //        .build();
        //StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);
        //
        ////1.2基于老版本planner的批处理
        //ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        //BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        //1.3基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                //.useBlinkPlanner() //The old planner has been removed in Flink 1.14.
                // Since there is only one planner left (previously called the 'blink' planner),
                // this setting is obsolete and will be removed in future versions.
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        //1.4基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                //.useBlinkPlanner() //The old planner has been removed in Flink 1.14.
                // Since there is only one planner left (previously called the 'blink' planner),
                // this setting is obsolete and will be removed in future versions.
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);


    }
}
