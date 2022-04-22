package com.task;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindow extends ProcessWindowFunction<MessageModel, String, Integer, TimeWindow> {

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<MessageModel> elements,
                        Collector<String> out) throws Exception {
        int sum=0;
        for (MessageModel item: elements) {
            sum+=item.Quantity;
        }
        String kq = "[{\"Id\": \""+key+"\"," +
                "\"Quantity\": \""+sum+"\"" +
                "}]";
//        System.out.println(kq);
        out.collect(kq);
    }
}
