package io.java.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    public static void main(String[] args) {
        log.info("Hello world!");

        // create Producer properties

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        // create a producer record

        ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo-java" ,"Hello_world");

        // send data - asynchronous

        producer.send(producerRecord);

        // flush data - synchronous

        producer.flush();

        // flush and close producer

        producer.close();

    }
}