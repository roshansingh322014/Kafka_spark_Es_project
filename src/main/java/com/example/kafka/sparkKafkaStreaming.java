package com.example.kafka;



import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.Collections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class sparkKafkaStreaming {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
//        Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2").option("subscribe", "topic1").load();
//        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        System.out.println("Spark Streaming started now ...");

        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // batchDuration
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(60000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("kafka-spark");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("RDD Count (Records Arrived)"+rdd.count());
            System.out.println("RDD Partitions"+rdd.partitions().size());
            if(rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {
                 System.out.println(rawRecord);
                });
            }
        });
        ssc.start();
       //ssc.awaitTermination();
    }

}
