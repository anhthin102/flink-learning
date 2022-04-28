package com.task.windowtime.task2;

import com.task.MessageModel;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;

public class CustomSessionTrigger extends Trigger<MessageModel, TimeWindow> {
    private final long timeout;

    private final ValueStateDescriptor<Boolean> cleanupStateDescriptor =
            new ValueStateDescriptor<>("cleanupState", Boolean.class, false);
    public CustomSessionTrigger(Time timeout) {
        this.timeout = timeout.toMilliseconds();
    }

    @Override
    public TriggerResult onElement(MessageModel messageModel, long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        // Check if we already triggered the cleanup (in this case we don't want to fire)
        ValueState<Boolean> cleanupState = triggerContext.getPartitionedState(cleanupStateDescriptor);
        if(cleanupState.value()) {
            return TriggerResult.CONTINUE;
        }

        // register a time at the end of the window
        if (triggerContext.getCurrentWatermark() < window.maxTimestamp()) {
            triggerContext.registerEventTimeTimer(window.maxTimestamp());
        }

        // fire on every element
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext triggerContext) throws Exception {
        if (time == window.maxTimestamp()) {
            triggerContext.registerEventTimeTimer(window.maxTimestamp() + timeout);
            return TriggerResult.FIRE;
        } else if (time > window.maxTimestamp()){
            System.out.println("time: " + time);
            System.out.println("maxTimeStamp: " + window.maxTimestamp());
            // purging and sending out 0 element
            try {
                ValueState<Boolean> cleanupState = triggerContext.getPartitionedState(cleanupStateDescriptor);
                cleanupState.update(true);
            } catch (IOException e) {
                throw new RuntimeException("Failed to update state", e);
            }
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteEventTimeTimer(window.maxTimestamp());
    }
}
