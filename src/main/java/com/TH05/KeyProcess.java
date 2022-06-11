package com.TH05;

import com.task.MessageModel;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.IOException;

public class KeyProcess extends KeyedProcessFunction<Integer, MessageModel, Tuple2<Integer, Integer>> {

    private transient ValueState<Integer> sum;
    private transient  ValueState<Long> state;
    private transient ValueState<MessageModel> mes;

    @Override
    public void open(Configuration parameters) throws Exception {
        sum = getRuntimeContext().getState(new ValueStateDescriptor<>("myQuantity", Integer.class));
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Long.class));
        mes = getRuntimeContext().getState(new ValueStateDescriptor<>("myMes", MessageModel.class));
    }

    @Override
    public void processElement(MessageModel messageModel, KeyedProcessFunction<Integer, MessageModel, Tuple2<Integer, Integer>>.Context context, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
        // retrieve the current quantity
        ObjectMapper mapper = new ObjectMapper();
        try {

            int quantity=sum.value()==null?messageModel.Quantity:messageModel.Quantity+sum.value();

            // set the state's timestamp to the record's assigned event time timestamp
            long lastModified=context.timestamp();
            System.out.println("lastModified "+lastModified+
                    " id"+ messageModel.Id+
                    "sum"+quantity);


            // write the state back
            sum.update(quantity);
            state.update(lastModified);
            mes.update(new MessageModel(messageModel.Id,quantity));

            // schedule the next timer 60 seconds from the current event time
            context.timerService().registerProcessingTimeTimer(lastModified+10000);
//            collector.collect(new Tuple2<Integer, Integer>(mes.value().Id, quantity));
        } catch (IOException e) {

            e.printStackTrace();

        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Integer,
            MessageModel, Tuple2<Integer, Integer>>.OnTimerContext ctx,
                        Collector<Tuple2<Integer, Integer>> out) throws Exception {
        // get the state for the key that scheduled the timer
        Long result = state.value();

        // check if this is an outdated timer or the latest timer
        System.out.println("result "+result+" timestamp "+timestamp+
                " id"+mes.value().Id+" sum"+sum.value());
        System.out.println();
        if (timestamp == result + 10000) {
            // emit the state on timeout
            out.collect(new Tuple2<Integer, Integer>(mes.value().Id, sum.value()));
            System.out.println("OUT COLLECT "+mes.value().Id+" - "+sum.value());

        }
    }
}
