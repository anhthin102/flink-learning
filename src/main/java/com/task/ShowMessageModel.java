package com.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ShowMessageModel implements FlatMapFunction<MessageModel, MessageModel> {
@Override
public void flatMap(MessageModel value, Collector<MessageModel> out) {
        System.out.println(value.toString());
        // convert string to my object
        out.collect(value);
        }
}