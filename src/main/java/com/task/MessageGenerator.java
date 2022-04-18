package com.task;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MessageGenerator implements SourceFunction<String> {

    public static final int SLEEP_MILLIS_PER_EVENT = 10;
    private static final int BATCH_SIZE = 5;
    private static final int NUMBER_OF_MESSAGE = 10;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int count = 0;

        while (running&&count<NUMBER_OF_MESSAGE) {

            for (int i = 1; i <= BATCH_SIZE; i++) {
                MessageModel message = new MessageModel();
                sourceContext.collect(message.toString());
                System.out.println(message.toString());
            }
            // prepare for the next batch
            count += BATCH_SIZE;
            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }

        sourceContext.close();
    }

    @Override
    public void cancel() {
        running=false;
    }
}
