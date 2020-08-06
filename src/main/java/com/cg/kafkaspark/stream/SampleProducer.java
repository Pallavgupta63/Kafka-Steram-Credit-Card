package com.cg.kafkaspark.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import java.util.*;
import java.nio.*;
import java.nio.file.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

public class SampleProducer {

    public SampleProducer()
    {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        try {
            List<String> lines = Files.readAllLines(Paths.get("C:\\Users\\Sanjay Gupta\\Desktop\\Capgemini\\user.csv"));
            int i=0;
            for (String line : lines) {
                if(i%10==0 && i!=0)
                {
                    Date date = Calendar.getInstance().getTime();
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
                    String strDate = dateFormat.format(date);
                    ProducerRecord producerRecord = new ProducerRecord("channel","name",strDate);

                    KafkaProducer kafkaProducer = new KafkaProducer(prop);

                    kafkaProducer.send(producerRecord);
                    kafkaProducer.close();
                    Thread.sleep(10000);

                }
                line = line.replace("\"", "");
                ProducerRecord producerRecord = new ProducerRecord("channel","name",line);

                KafkaProducer kafkaProducer = new KafkaProducer(prop);

                kafkaProducer.send(producerRecord);
                kafkaProducer.close();
                i++;
            }
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }

    }
}
