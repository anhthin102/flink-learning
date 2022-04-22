package com.task.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Map;

public class pusToHazelcastv1 implements FlatMapFunction<Tuple2<Integer, Integer>, String> {
    @Override
    public void flatMap(Tuple2<Integer, Integer> t, Collector<String> out) {
        String kq = "[{\"Id\": \""+t._1+"\"," +
                "\"Quantity\": \""+t._2+"\"" +
                "}]";
        Config cfg = new Config();
        cfg.getNetworkConfig()
                .setPublicAddress( "10.1.6.216:5701" );
        cfg.setInstanceName( "messageFlink" );
        // Start the client and connect to the cluster
        Map<Integer, Integer> mapMessageFlink;
        if(Hazelcast.getHazelcastInstanceByName( "messageFlink" )==null){
            HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
        }
        else{
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName( "messageFlink" );
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
        }
        mapMessageFlink.put(t._1, t._2);
//        System.out.println("Message with Id "+ t._1+ ": "+mapMessageFlink.get(t._1));
//        System.out.println("Map Size:" + mapMessageFlink.size());
        out.collect(kq);

    }
}
