package com.task.hazelcast;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Map;

public class pusToHazelcastv2 extends RichFlatMapFunction<Tuple2<Integer, Integer>, String> {
    Map<Integer, Integer> mapMessageFlink;

    @Override
    public void flatMap(Tuple2<Integer, Integer> t, Collector<String> out) {
        String kq = "[{\"Id\": \""+t._1+"\"," +
                "\"Quantity\": \""+t._2+"\"" +
                "}]";

        mapMessageFlink.put(t._1, t._2);
        for (Map.Entry<Integer, Integer> entry : mapMessageFlink.entrySet()) {
            Integer k = entry.getKey();
            Integer v = entry.getValue();
            System.out.println("key: " + k + ", value: " + v);
        }
/*        System.out.println( "Map Size:" + mapMessageFlink.size() );
        System.out.println("Message with Id "+ t._1+ ": "+mapMessageFlink.get(t._1));*/
//        System.out.println("Map Size:" + mapMessageFlink.size());*/
        out.collect(kq);

    }

    @Override
    public void open(Configuration config) {
        /*Config cfg = new Config();
        cfg.getNetworkConfig()
                .setPublicAddress( "10.1.6.216:5701" );
        cfg.setInstanceName( "messageFlink" );
        // Start the client and connect to the cluster

        if(Hazelcast.getHazelcastInstanceByName( "messageFlink" )==null){
            HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
        }
        else{
            HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName( "messageFlink" );
            // Create a Distributed Map in the cluster
            mapMessageFlink = instance.getMap("messageFlink");
        }*/

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress("10.1.6.216:5701");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        mapMessageFlink = client.getMap("messageFlink");

    }
}
