package com.task.windowtime.task;

import com.task.MessageModel;
import com.task.timer.org.CountWithTimestamp;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;

public class CustomTimeTrigger extends org.apache.flink.streaming.api.windowing.triggers.Trigger<MessageModel, TimeWindow> {
    @Override
    public TriggerResult onElement(MessageModel messageModel, long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        if (window.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            triggerContext.registerEventTimeTimer(window.maxTimestamp());
            //triggerContext.registerProcessingTimeTimer(l+30000);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        if (l == window.maxTimestamp()) {
            triggerContext.registerEventTimeTimer(window.maxTimestamp() + 30000);
            return TriggerResult.FIRE;
        } else
            return TriggerResult.CONTINUE;

    }

    @Override
    public void clear(TimeWindow window, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteEventTimeTimer(window.maxTimestamp());
    }


}
