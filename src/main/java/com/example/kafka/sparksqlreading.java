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
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

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

       //Dataset<Row> filtersecondexample=dataset.filter(column("gender").equalTo("female").and(column("writing score").geq(80)));
        //filtersecondexample.show();
        spark.close();
    }
}
