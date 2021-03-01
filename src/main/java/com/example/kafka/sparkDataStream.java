package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class sparkDataStream {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // this class if for Discretised streaming


//        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));




    }
}
