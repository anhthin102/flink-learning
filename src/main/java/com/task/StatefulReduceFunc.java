package com.task;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import javax.security.auth.login.Configuration;

public class StatefulReduceFunc extends KeyedProcessFunction<Integer, MessageModel, Tuple2<Integer, Integer>> {


    private transient ValueState<Integer> sum;
    @Override
    public void processElement(MessageModel item, KeyedProcessFunction<Integer, MessageModel, Tuple2<Integer, Integer>>.Context context, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        int quantity= sum.value()==null?0:item.Quantity+sum.value();
        out.collect(new Tuple2<Integer, Integer>(item.Id,quantity));
    }
     public void open(Configuration parameters){
         ValueStateDescriptor<Integer> valueStateDescriptor=new ValueStateDescriptor<>("sum",Integer.class);
         sum=getRuntimeContext().getState(valueStateDescriptor);
     }


}
