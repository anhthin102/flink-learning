package com.task;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import scala.Tuple2;

public class StatefulTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(2*60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.setParallelism(2);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(parameterTool);
        FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducer(parameterTool);
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);

        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer ());
        message = message.flatMap(new ShowMessageModel());
        DataStream<Tuple2<Integer, Integer>> groupMessage= message.keyBy(new KeyByDescription())
                .process(new StatefulReduceFunc());
        DataStream<String> stringGroupMessage = groupMessage.flatMap(new convertTupleToString());
        stringGroupMessage.addSink(flinkKafkaProducer);
        groupMessage.print();
//        groupMessage.addSink(flinkKafkaProducer);
        env.execute();

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
