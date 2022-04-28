package com.task.windowtime.org;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;


public class CustomTrigger {


    public static void main(String[] args) throws Exception {

        // timeout after which sent a 0 element
        final Time timeout = Time.milliseconds(5);

        // initialize a new Collection-based execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env
                //Create DataStreamSource from 0 to 100
                .generateSequence(0, 100)
                .assignTimestampsAndWatermarks(new CustomAssigner())
                //Passing value Long to map function
                .map(new MapFunction<Long, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Long value) throws Exception {
                        return new Tuple2<>(1L, value);
                    }
                })
                .keyBy(0)
                //Session Window With Boundary 5s
                .window(EventTimeSessionWindows.withGap(timeout))
                .allowedLateness(timeout)
                .trigger(new CustomEventTrigger(timeout))
                .apply(new CustomWindowFunction())
                .print();


        env.execute();
    }


    private static class CustomAssigner implements AssignerWithPunctuatedWatermarks<Long> {

        private long lastWaterMark = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
            if (extractedTimestamp % 10 == 0) {
                lastWaterMark = lastElement;
            }
            System.out.println("watermark: " + lastWaterMark);
            return new Watermark(lastWaterMark);
        }

        @Override
        public long extractTimestamp(Long element, long previousElementTimestamp) {
            if (element > 50) {
                return element + 10;
            }
            return element;
        }
    }

    private static class CustomEventTrigger extends EventTimeTrigger {

        private final long timeout;

        private final ValueStateDescriptor<Boolean> cleanupStateDescriptor =
                new ValueStateDescriptor<>("cleanupState", Boolean.class, false);

        private CustomEventTrigger(Time timeout) {
            this.timeout = timeout.toMilliseconds();
        }

        //It is triggered every time an element is added to the window.
        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            // Check if we already triggered the cleanup (in this case we don't want to fire)
            ValueState<Boolean> cleanupState = ctx.getPartitionedState(cleanupStateDescriptor);
            if(cleanupState.value()) {
                return TriggerResult.CONTINUE;
            }

            // register a time at the end of the window
            if (ctx.getCurrentWatermark() < window.maxTimestamp()) {
                ctx.registerEventTimeTimer(window.maxTimestamp());
            }

            // fire on every element
            return TriggerResult.FIRE;
        }

        //It called when Event-Time timer is triggered.
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            if (time == window.maxTimestamp()) {
                ctx.registerEventTimeTimer(window.maxTimestamp() + timeout);
                return TriggerResult.FIRE;
            } else if (time > window.maxTimestamp()){
                System.out.println("time: " + time);
                System.out.println("maxTimeStamp: " + window.maxTimestamp());
                // purging and sending out 0 element
                try {
                    ValueState<Boolean> cleanupState = ctx.getPartitionedState(cleanupStateDescriptor);
                    cleanupState.update(true);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to update state", e);
                }
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                return TriggerResult.CONTINUE;
            }
        }
    }

    private static class CustomWindowFunction extends RichWindowFunction<Tuple2<Long, Long>, Long, Tuple, TimeWindow> {

        /** State to check if we should clean up and send the zero element */
        private transient ValueState<Boolean> cleanupState;

        /** State to keep track of our current count */
        private transient ValueState<Long> countState;

        private final ValueStateDescriptor<Boolean> cleanupStateDescriptor =
                new ValueStateDescriptor<>("cleanupState", Boolean.class, false);

        private final ValueStateDescriptor<Long> counterStateDescriptor =
                new ValueStateDescriptor<>("counterState", Long.class, 0L);

        @Override
        public void open(Configuration parameters) throws Exception {
            cleanupState = getRuntimeContext().getState(cleanupStateDescriptor);
            countState = getRuntimeContext().getState(counterStateDescriptor);
        }

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Long, Long>> input, Collector<Long> out) throws Exception {
            if (cleanupState.value()) {
                // we should clean up and send out the zero element
                out.collect(-1L);
            } else {
                // regular window function
                // do some computation
                long newCount = 0;
                for (Tuple2<Long, Long> elem : input) {
                    newCount = elem.f1;
                }

                // check if state was updated and only emit then
                if (countState.value() != newCount) {
                    countState.update(newCount);
                    out.collect(newCount);
                }


            }
        }

    }

    private static class EventTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private EventTimeTrigger() {}

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp() ?
                    TriggerResult.FIRE :
                    TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }

    }

}
