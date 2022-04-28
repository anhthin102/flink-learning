package com.task.windowtime;

import com.task.MessageModel;
import org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.curator4.org.apache.curator.connection.ConnectionHandlingPolicy;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.lang.reflect.Field;

public class EventTimeProcessingTimeTrigger extends Trigger< MessageModel, TimeWindow> {

    private final long sessionTimeout;

    // End session events
    private static ImmutableSet<String> endSession = ImmutableSet.<String>builder()
            .add("Playback.Aborted")
            .add("Playback.Completed")
            .add("Playback.Error")
            .add("Playback.StartAirplay")
            .add("Playback.StartCasting")
            .build();

    public EventTimeProcessingTimeTrigger(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }


    @Override
    public TriggerResult onElement(MessageModel messageModel, long l, TimeWindow window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
