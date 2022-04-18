package com.task;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class Kafka_Flink_Sender {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamSender(parameterTool);

    }
    public static void StreamSender(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> stringOutputStream = environment.addSource(new StreamGenerator());
        DataStream<String> stringOutputStream = environment.addSource(new MessageGenerator());

        FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducer(
                parameterTool);

        stringOutputStream.addSink(flinkKafkaProducer);

        environment.execute();
    }
    public static class StreamGenerator implements SourceFunction<String> {

        boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int counter = 0;
            while(flag) {
                ctx.collect("From Flink : "+ counter++);
                System.out.println("From Flink : "+ counter);
                Thread.sleep(1000);
            }
            ctx.close();
        }

        @Override
        public void cancel() {
            flag = false;

        }

    }
    public static FlinkKafkaProducer<String> createStringProducer(
            ParameterTool parameterTool){

        return new FlinkKafkaProducer<>(parameterTool.getRequired("bootstrap.servers"),parameterTool.getRequired("topic"),
                new SimpleStringSchema());
    }

}
