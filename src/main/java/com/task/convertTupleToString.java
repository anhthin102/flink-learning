package com.task;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class convertTupleToString implements FlatMapFunction<Tuple2<Integer, Integer>, String> {
    @Override
    public void flatMap(Tuple2<Integer, Integer> t, Collector<String> out) {
        String kq = "[{\"Id\": \""+t._1+"\"," +
                "\"Quantity\": \""+t._2+"\"" +
                "}]";
        out.collect(kq);

    }
}