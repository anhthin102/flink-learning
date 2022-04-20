package com.task;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;

public class ProcessWindow1 extends ProcessWindowFunction<MessageModel, String, Integer, TimeWindow> {

    private transient ValueState<Integer> quantity;
    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<MessageModel> elements,
                        Collector<String> out) throws Exception {
        int sum = 0;
        for (MessageModel item: elements) {
            sum= quantity.value()==null?0:item.Quantity+quantity.value();
        }
        String kq = "[{\"Id\": \""+key+"\"," +
                "\"Quantity\": \""+sum+"\"" +
                "}]";
        out.collect(kq);
    }
    public void open(Configuration parameters){
        ValueStateDescriptor<Integer> valueStateDescriptor=new ValueStateDescriptor<>("quantity",Integer.class);
        quantity=getRuntimeContext().getState(valueStateDescriptor);
    }
}
