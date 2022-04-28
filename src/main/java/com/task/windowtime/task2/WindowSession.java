package com.task.windowtime.task2;

import com.task.KeyByDescription;
import com.task.MessageModel;
import com.task.Tokenizer;
import com.task.windowtime.task.CustomTimeTrigger;
import com.task.windowtime.task.ProcessWindowTrigger;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WindowSession {
    public static void main(String[] args) throws Exception {
        // timeout after which sent a 0 element
        final Time timeout = Time.milliseconds(5);
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_topic3", new SimpleStringSchema(), properties));
//        stream.print();
        DataStream<MessageModel> message = stream.flatMap(new Tokenizer());
        //DataStream<Tuple2<Integer, Integer>> result =
                message
                        .keyBy(new KeyByDescription())
                        .window(EventTimeSessionWindows.withGap(timeout))
                        .trigger(new CustomSessionTrigger(timeout));
        env.execute();
    }

}
