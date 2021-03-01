package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.column;

public class sparksqlreading {



    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark= SparkSession.builder().appName("testingsql").master("local[*]")
                .getOrCreate();

        // .config("spark.sql.warehouse.dir","file:///c:/tmp/")

        Dataset<Row> dataset=spark.read().option("header",true).csv("Data/student.csv");
        long numberofRows=dataset.count();
        System.out.println("There are " + numberofRows + "records");
        dataset.show();

        Row firstRow=dataset.first();

        String datatype=firstRow.getAs("math score").toString();
        System.out.println(datatype);

        int num=Integer.parseInt(firstRow.getAs("reading score"));
        System.out.println(num);

        Dataset<Row> filterbyoccupation=dataset.filter("gender='female'");
        filterbyoccupation.show();

        //Creating the view on the existing table
        dataset.createOrReplaceTempView("my_student_table");

        // Aggregation method no 1
        Dataset<Row> result1 = spark.sql("select * from my_student_table where gender='female'");
        result1.show();


        Dataset<Row> result2 = spark.sql("select avg(`math score`),avg(`reading score`),avg(`writing score`) from my_student_table ");
        result2.show();

        Dataset<Row> result3 = spark.sql("select max(`math score`),max(`reading score`),max(`writing score`) from my_student_table");
        result3.show();

        spark.close();
    }
}
