package com.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Tokenizer implements FlatMapFunction<String, MessageModel> {
    @Override
    public void flatMap(String value, Collector<MessageModel> out) {
        ObjectMapper mapper = new ObjectMapper();
        try {

            List<MessageModel> serviceModels = Arrays.asList(mapper.readValue(value, MessageModel[].class));

            for (MessageModel item: serviceModels) {
                // convert string to my object
                out.collect(item);
            }

        } catch (IOException e) {

            e.printStackTrace();

        }

    }
}


