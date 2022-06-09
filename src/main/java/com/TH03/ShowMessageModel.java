package com.TH03;

import com.task.MessageModel;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ShowMessageModel implements FlatMapFunction<MessageModel, String> {
@Override
public void flatMap(MessageModel value, Collector<String> out) {
        System.out.println(value.toString());
        // convert string to my object
        out.collect(value.toString());
        }
}