package com.task.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.task.MessageModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class ShowMessageFromHazelcast implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> out) {
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
            System.out.println("Khong co tin nhan o day");
        }
        else{
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName( "messageFlink" );
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
            for (Map.Entry<Integer, Integer> entry : mapMessageFlink.entrySet()) {
                Integer k = entry.getKey();
                Integer v = entry.getValue();
                System.out.println("key: " + k + ", value: " + v);
            }
            System.out.println( "Map Size:" + mapMessageFlink.size() );
        }
        System.out.println(value);
        out.collect(value);
    }
}