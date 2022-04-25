package com.task.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.task.MessageModel;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;


public class hazelCastProcessWindow extends ProcessWindowFunction<MessageModel, String, Integer, TimeWindow> {

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<MessageModel> elements,
                        Collector<String> out) throws Exception {

        int sum=0;
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
        for (MessageModel item: elements) {
            sum+=item.Quantity;
        }
        mapMessageFlink.put(key, sum);
        System.out.println("Message with Id 1: "+ mapMessageFlink.get(1));
        System.out.println("Map Size:" + mapMessageFlink.size());
        String kq = "[{\"Id\": \""+key+"\"," +
                "\"Quantity\": \""+sum+"\"" +
                "}]";
        out.collect(kq);
    }
}