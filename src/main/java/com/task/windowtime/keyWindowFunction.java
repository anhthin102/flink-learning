package com.task.windowtime;

import com.task.KeyByDescription;
import com.task.MessageModel;
import com.task.ProcessWindow;
import com.task.Tokenizer;
import com.task.timer.org.CountWithTimeoutFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.shaded.curator4.org.apache.curator.connection.ConnectionHandlingPolicy.CheckTimeoutsResult.SESSION_TIMEOUT;

public class keyWindowFunction {
    public static void main(String[] args) throws Exception {
        // the source data stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.12.183:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);
        env.getCheckpointConfig().setCheckpointTimeout(2*60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_topic3", new SimpleStringSchema(), properties));
        stream.print();
        DataStream<MessageModel> message = stream.flatMap(new Tokenizer());
        //message.print();
        DataStream<String> groupMessage= message.keyBy(new KeyByDescription())
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(60)))
                .process(new ProcessWindow());
        message
                .keyBy(new KeyByDescription())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                        .trigger(new EventTimeProcessingTimeTrigger(5000));
//        result.print();
        env.execute();
    }
}
