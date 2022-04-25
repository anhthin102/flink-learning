package com.task.gethazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.Map;

public class getMessageHazelcast {

    public static void main(String[] args) {
        Map<Integer, Integer> mapMessageFlink;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress("10.1.6.216:5701");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        mapMessageFlink = client.getMap("messageFlink");
        for (Map.Entry<Integer, Integer> entry : mapMessageFlink.entrySet()) {
            Integer k = entry.getKey();
            Integer v = entry.getValue();
            System.out.println("key: " + k + ", value: " + v);
        }
        System.out.println( "Map Size:" + mapMessageFlink.size() );
    }
}
