package com.TH03;

import com.task.MessageModel;

public class NYCFilter implements org.apache.flink.api.common.functions.FilterFunction<com.task.MessageModel> {
    @Override
    public boolean filter(MessageModel messageModel) throws Exception {
        return messageModel.Quantity>500;
    }
}
