package com.TH05;

import com.task.KeyByDescription;
import com.task.MessageModel;
import com.task.Tokenizer;
import com.task.timer.reduceWithTimeoutFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple2;

import java.util.Properties;

public class Task2 {
    public static void main(String[] args) throws Exception {
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("thin-TH02-topic1", new SimpleStringSchema(), properties));
        stream.print();
        DataStream<MessageModel> message = stream.flatMap(new Tokenizer());
        DataStream<Tuple2<Integer, Integer>> groupMessage = message
                .keyBy(new KeyByDescription())
                .process(new KeyProcess());
//        groupMessage.print();
        env.execute();
    }
}