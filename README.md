# Kafka_spark_Es_project

1). Overview of different programs in the Source folder 


 Project is related to integration of kafka,spark and elastic search and processing of data.


Producer.java, JsonSerializer.java,studentModel.java  ---> " these programs is used to send the json data to the 3 partition kafka topic .

StructuredStreamingJava.java                          ---> " this class process the real time data from the kafka topic and writes into a local file under the data folder"

readingTexttoSpark.java                               ---> " This program read the text data from local file system and then aggregate it and finally store it into another local file system"

sparkDataStream.java                                  ---->" this program read the .csv file from local file sytem and put it into the Elastic Search which can we seen from kibana"

sparksqlreading.java                                  ---->" this program read and .csv file and apply vairous types of sql queris and functions to get various insights "




