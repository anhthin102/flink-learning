package com.task.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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

import java.util.Map;

public class uploadMessageToHazelcastFix {
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
        DataStream<String> stringInputStream = env.addSource(flinkKafkaConsumer);

        DataStream<MessageModel> message = stringInputStream.flatMap(new Tokenizer());
        message = message.flatMap(new ShowMessageModel());
        DataStream<Tuple2<Integer, Integer>> groupMessage= message.keyBy(new KeyByDescription())
                .process(new StatefulReduceFunc());
        DataStream<String> stringGroupMessage = groupMessage.flatMap(new pusToHazelcastv2());
        stringGroupMessage = stringGroupMessage.flatMap(new ShowMessageFromHazelcast());
//        groupMessage.print();
//        groupMessage.addSink(flinkKafkaProducer);


        /*Config cfg = new Config();
        cfg.getNetworkConfig()
                .setPublicAddress( "10.1.6.216:5701" );
        cfg.setInstanceName( "messageFlink" );
        // Start the client and connect to the cluster
        Map<Integer, Integer> mapMessageFlink;
        if(Hazelcast.getHazelcastInstanceByName( "messageFlink" )==null){
            HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
            System.out.println("Khong co tin nhan o day");
        }
        else{
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName( "messageFlink" );
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
            System.out.println( "Map Size:" + mapMessageFlink.size() );
        }*/
        env.execute();

    }
    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            ParameterTool parameterTool) {
        FlinkKafkaConsumer<String> consumer = new
                FlinkKafkaConsumer<String>(parameterTool.getRequired("input-topic"),
                new SimpleStringSchema(), parameterTool.getProperties());

        return consumer;
    }
}
