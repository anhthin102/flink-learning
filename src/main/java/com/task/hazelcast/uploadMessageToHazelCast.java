package com.task.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.task.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Map;
import java.util.Queue;

public class uploadMessageToHazelCast {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamStringOperation(parameterTool);

    }
    public static void StreamStringOperation(ParameterTool parameterTool ) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(parameterTool);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
//        stringInputStream.print();
        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer ());
        message = message.flatMap(new ShowMessageModel());

        DataStream<String> groupMessage= message.keyBy(new KeyByDescription())
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
                .process(new hazelCastProcessWindow());

        environment.execute();
    }
    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            ParameterTool parameterTool) {
        FlinkKafkaConsumer<String> consumer = new
                FlinkKafkaConsumer<String>(parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        return consumer;
    }
}
