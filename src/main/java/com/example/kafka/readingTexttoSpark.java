package com.example.kafka;

//import org.apache.logging.log4j.Level;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Serializable;
import scala.Tuple2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.commons.io.FileUtils;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import org.apache.commons.cli.*;

public class readingTexttoSpark implements Serializable
{
    //private static final Pattern SPACE = Pattern.compile(" ");

    @SuppressWarnings("resource")

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);





        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("StartingSpark ").setMaster("local");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(  "file:///C:/Users/rosha/IdeaProjects/Kafka_spark_streaming/Data/inputdemo.txt")   ;// collect RDD for printing

       // lines.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        JavaRDD<String> letteronly=lines.map(sentence -> sentence.replaceAll("[^a-zA-Z ]"," ").toLowerCase());

        JavaRDD<String> justwords=letteronly.flatMap(sentence  -> Arrays.asList(sentence.split(" ")).iterator());
        JavaPairRDD<String,Integer> pairRDD=justwords.mapToPair(word -> new Tuple2<String, Integer>(word,1));
        //JavaPairRDD<String,Integer> totals = pairRDD.((value1,value2)-> value1+ value2);
        //JavaPairRDD countData = pairRDD.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);


       // pairRDD.coalesce(1);
        pairRDD.foreach(element -> System.out.println(element));


       // List <String> result = justwords.take(50);
       // result.forEach(System.out::println);

        //List <Tuple2<String,Integer>> result =pairRDD.take(20);
        //result.forEach(System.out::println);
      pairRDD.saveAsTextFile("file:///C:/Users/rosha/IdeaProjects/Kafka_spark_streaming/Data/reading_text_data");


        sc.close();



    }

    //for(String line:counts.collect()){
       //    System.out.println(line);
   // }
    }




