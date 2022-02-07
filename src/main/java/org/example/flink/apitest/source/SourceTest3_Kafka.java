package org.example.flink.apitest.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从Kafka中读取数据
 *
 * kafka在终端生产消息
 * kafka-console-producer.sh --broker-list 172.16.136.128:9092 --topic sensor
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取数据
        //FlinkKafkaConsumer is deprecated and will be removed with Flink 1.15, please use KafkaSource instead.
        //https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#kafka-sourcefunction
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("172.16.136.128:9092")
                .setTopics("sensor")
                .setGroupId("consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build()
                ;

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source");

        //打印输出
        dataStream.print();

        //执行
        env.execute();
    }
}
