package com.task.timer.org;

import com.task.MessageModel;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Integer, MessageModel, Tuple2<Integer, Integer>> {

    /** The state that is maintained by this process function */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(
            MessageModel value,
            Context ctx,
            Collector<Tuple2<Integer, Integer>> out) throws Exception {
        //System.out.println("Id: "+value.Id);
        //System.out.println("Quantity: "+value.Quantity);

        // retrieve the current count
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.Id;
            current.count=0;
        }

        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        //ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
        ctx.timerService().registerProcessingTimeTimer(current.lastModified + 30000);
        /*if(current.key==1){
            System.out.println("id=1");
        }*/
//        out.collect(new Tuple2<Integer, Integer>(current.key, current.count));
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<Integer, Integer>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        System.out.println("TimerNoCondition");
        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 30000) {
            // emit the state on timeout
            System.out.println("TimerWithCondition");
            out.collect(new Tuple2<Integer, Integer>(result.key, result.count));
        }
    }
}
