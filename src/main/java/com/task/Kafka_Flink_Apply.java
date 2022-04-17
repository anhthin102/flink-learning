package com.task;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Kafka_Flink_Apply {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamStringOperation(parameterTool );

    }

    public static void StreamStringOperation(ParameterTool parameterTool ) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(parameterTool);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
//        stringInputStream.print();
        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer ());
        message = message.flatMap(new ShowMessageModel());
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
