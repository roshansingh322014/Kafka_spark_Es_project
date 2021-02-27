package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.Level;
//import org.apache.log4j.Logger;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR);

        // Creating a loger to store metadata about the file

        final Logger logger= LoggerFactory .getLogger(Producer.class);




        // Creating the property object for the producer
        Properties prop= new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class.getName());

        // Creating the student object

        studentModel student= new studentModel(3233, "rks","cs");





        // Create the producer

        final KafkaProducer<String,studentModel> producer= new KafkaProducer<String, studentModel>(prop);


        // Create a producer record

        ProducerRecord<String, studentModel> record= new ProducerRecord<String,studentModel>("kafka-spark","key1", student);

        //Send data in Asynchronous mode

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    logger.info("\nRecieved record metadata: \n"+"Topic:" + recordMetadata.topic());

                }
                else {
                    logger.error("Error Occured ",e);
                }
            }
        });

        //flush and close producer

        //producer.flush();
        producer.close();



    }

}
