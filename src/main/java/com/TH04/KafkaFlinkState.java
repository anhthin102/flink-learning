package com.TH04;

import com.task.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import scala.Tuple2;

import java.util.Properties;

public class KafkaFlinkState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "group1");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer <>("thin-TH02-topic1",  new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> flinkKafkaProducer =  new FlinkKafkaProducer<>("10.1.12.183:9092",
                "thin-TH04-topic1",
                new SimpleStringSchema());
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);
        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer());
        message = message.flatMap(new ShowMessageModel());
        DataStream<Tuple2<Integer, Integer>> groupMessage= message.keyBy(new KeyByDescription())
                .process(new StatefulReduceFunc());
        DataStream<String> stringGroupMessage = groupMessage.flatMap(new convertTupleToString());
        stringGroupMessage.addSink(flinkKafkaProducer);
        groupMessage.print();
        env.execute();

    }

}
