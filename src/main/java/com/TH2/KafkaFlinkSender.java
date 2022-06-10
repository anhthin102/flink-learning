package com.TH2;

import com.task.MessageGenerator;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaFlinkSender {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringOutputStream = environment.addSource(new MessageGenerator());

        FlinkKafkaProducer<String> flinkKafkaProducer =  new FlinkKafkaProducer<>("10.1.12.183:9092",
                "thin-TH02-topic1",
                new SimpleStringSchema());

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
}
