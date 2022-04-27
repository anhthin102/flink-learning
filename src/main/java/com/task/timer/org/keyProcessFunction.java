package com.task.timer.org;

import com.task.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class keyProcessFunction {
    public static void main(String[] args) throws Exception {
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_topic3", new SimpleStringSchema(), properties));
        stream.print();
        DataStream<MessageModel> message = stream.flatMap(new Tokenizer());
        //message.print();
        DataStream<Tuple2<Integer, Integer>> result = message
                .keyBy(new KeyByDescription())
                .process(new CountWithTimeoutFunction());
        result.print();
        env.execute();
    }
}
