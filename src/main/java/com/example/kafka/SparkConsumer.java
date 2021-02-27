package com.example.kafka;


import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class SparkConsumer {

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
//        Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2").option("subscribe", "topic1").load();
//        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        System.out.println("Spark Streaming started now ...");

        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
//        StreamingContext sc = new StreamingContext(spark.sparkContext(), new Duration(60000));
//        conf.set("es.index.auto.create", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
//        // batchDuration
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(60000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("kafka-spark");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        List<String> allRecord = new ArrayList<String>();
        FileWriter writer = new FileWriter("Data/KafkaTopicData.txt");
        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("RDD Count (Records Arrived) "+rdd.count());
            System.out.println("RDD Partitions "+rdd.partitions().size());
            if(rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {
//                    System.out.println(rawRecord);
                    allRecord.add(rawRecord._2());
                });
                System.out.println(allRecord);

                for(String s: allRecord) {
                    System.out.println(s);
                    writer.write(s);
                    writer.write("\n");
                }
                writer.close();
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}
