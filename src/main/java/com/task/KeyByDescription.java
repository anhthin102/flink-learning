package com.task;

import org.apache.flink.api.java.functions.KeySelector;

public class KeyByDescription implements KeySelector<MessageModel, Integer> {
    @Override
    public Integer getKey(MessageModel message) throws Exception {
        return message.Id;
    }
}