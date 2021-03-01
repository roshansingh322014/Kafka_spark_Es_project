package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.concurrent.ExecutorService;


public class StructuredStreamingJava {

    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session=SparkSession.builder()
                .master("local[*]")
                .appName("structuredviewingReport")
                .getOrCreate();

        Dataset<Row> df= session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "kafka-spark")
                .option("failOnDataLoss", "false")
                .load();

        df.createOrReplaceTempView("viewing_data");
        Dataset<Row> result = session.sql("select cast (value as string) as output from viewing_data output");

        session.conf().set("spark.sql.shuffle.partitions","10");
//        StreamingQuery query = result
//                .writeStream()
//                .format("console")
//                .trigger(Trigger.ProcessingTime("60 seconds"))
//                .outputMode(OutputMode.Complete())
//                .start();
        // can be "orc", "json", "csv", etc.
        // can be "orc", "json", "csv", etc.
        // can be "orc", "json", "csv", etc.
        StreamingQuery query = result.writeStream()
                .outputMode(OutputMode.Append())
                .option("checkpointLocation","checkpoints")
                .format("json")
                .option("truncate","false")
                .trigger(Trigger.ProcessingTime(60000))
                .option("path", "Data/kafka_batch_output")
                .start();

        //  .trigger(Trigger.ProcessingTime(80000))


        query.awaitTermination();
    }
}



