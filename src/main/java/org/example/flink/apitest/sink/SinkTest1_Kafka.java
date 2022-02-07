package org.example.flink.apitest.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.example.flink.apitest.beans.SensorReading;

/**
 * 发现问题，每次消费kafka里的数据，都是从新开始消费的，原因参考：
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#:~:text=Consumer%20Offset%20Committing-,%23,-Kafka%20source%20commits
 *
 * https://blog.csdn.net/qq_37332702/article/details/107617253
 *
 */
public class SinkTest1_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka中读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.16.136.128:9092")
                .setTopics("sensor")
                .setGroupId("consumer-group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        //转换成SensorReading类型
        DataStream<String> dataStream = sourceStream.map(line -> {
            System.out.println("-->"+line);
            String[] fields = line.split(",");
            if(fields.length == 3){
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
            }
            return null;
        });

        //输出到kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("172.16.136.128:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sinktest")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        dataStream.sinkTo(kafkaSink);

        //执行
        env.execute();
    }
}
