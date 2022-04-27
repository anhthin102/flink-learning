package com.task.timer;

import com.task.KeyByDescription;
import com.task.MessageModel;
import com.task.StatefulReduceFunc;
import com.task.Tokenizer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple2;

import java.util.Properties;

public class orgtimerprocessfuntion {
    public static void main(String[] args) throws Exception {
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_topic3", new SimpleStringSchema(), properties));
        stream.print();
        DataStream<MessageModel> message = stream.flatMap(new Tokenizer ());
        DataStream<Tuple2<Integer, Integer>> groupMessage = message
                .keyBy(new KeyByDescription())
                .process(new reduceWithTimeoutFunction());
        groupMessage.print();
        env.execute();
    }
}
