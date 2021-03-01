package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class sparkDataStream {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark= SparkSession.builder().appName("writeToElastic").master("local[*]")
                .getOrCreate();

        // .config("spark.sql.warehouse.dir","file:///c:/tmp/")

        Dataset<Row> dataset=spark.read().option("header",true).csv("Data/student.csv");
        long numberofRows=dataset.count();
        System.out.println("There are " + numberofRows + "records");
        dataset.show();

        dataset.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.node","localhost")
                .mode("append")
                .save("student/docs");



    }
}
