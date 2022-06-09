package com.TH03;

import com.task.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaFlinkFilter {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamStringOperation(parameterTool );

    }

    public static void StreamStringOperation(ParameterTool parameterTool ) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(parameterTool);
        FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducer(parameterTool);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
        stringInputStream.print();
        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer())
                .filter(new NYCFilter());
        DataStream<String> groupMessage = message.flatMap(new ShowMessageModel());
        groupMessage.addSink(flinkKafkaProducer);
        environment.execute();
    }



    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            ParameterTool parameterTool) {
        FlinkKafkaConsumer<String> consumer = new
                FlinkKafkaConsumer<String>(parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        return consumer;
    }
    public static FlinkKafkaProducer<String> createStringProducer(
            ParameterTool parameterTool){

        return new FlinkKafkaProducer<>(parameterTool.getRequired("bootstrap.servers"),parameterTool.getRequired("output-topic"),
                new SimpleStringSchema());
    }
}
