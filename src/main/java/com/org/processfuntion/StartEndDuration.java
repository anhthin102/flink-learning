package com.org.processfuntion;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**The following example shows a KeyedProcessFunction that operates on a KeyedStream
and matches START and END events. When a START event is received,
the function remembers its timestamp in state and registers a timer in four hours.
If an END event is received before the timer fires,
the function computes the duration between END and START event, clears the state, and returns the value.
Otherwise, the timer just fires and clears the state.


 * Matches keyed START and END events and computes the difference between
 * both elements' timestamps. The first String field is the key attribute,
 * the second String attribute marks START and END events.
 */

public class StartEndDuration
        extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

    private ValueState<Long> startTime;

    @Override
    public void open(Configuration conf) {
        // obtain state handle
        startTime = getRuntimeContext()
                .getState(new ValueStateDescriptor<Long>("startTime", Long.class));
    }

    /** Called for each processed event. */
    @Override
    public void processElement(
            Tuple2<String, String> in,
            Context ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        switch (in.f1) {
            case "START":
                // set the start time if we receive a start event.
                startTime.update(ctx.timestamp());
                // register a timer in four hours from the start event.
                ctx.timerService()
                        .registerEventTimeTimer(ctx.timestamp() + 4 * 60 * 60 * 1000);
                break;
            case "END":
                // emit the duration between start and end event
                Long sTime = startTime.value();
                if (sTime != null) {
                    out.collect(Tuple2.of(in.f0, ctx.timestamp() - sTime));
                    // clear the state
                    startTime.clear();
                }
            default:
                // do nothing
        }
    }

    /** Called when a timer fires. */
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) {

        // Timeout interval exceeded. Cleaning up the state.
        startTime.clear();
    }
}