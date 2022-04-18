package com.task;

import java.util.Random;

public class DataGenerator {
    private static final int NUMBER_OF_ID = 10;
    private  static  final int MAX_OF_QUANTTITY=1000;

    public DataGenerator(){

    }

    public int Id() {
        Random rnd = new Random();
        return rnd.nextInt( NUMBER_OF_ID-1 )+1;
    }

    public int Quantity() {
        Random rnd = new Random();
        return rnd.nextInt(MAX_OF_QUANTTITY);
    }

}
