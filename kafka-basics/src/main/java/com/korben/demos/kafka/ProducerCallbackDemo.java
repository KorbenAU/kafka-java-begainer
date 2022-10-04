package com.korben.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @ClassName ProducerDemo
 * @Description TODO
 * @Author Korben Gao
 * @Date 29/9/2022 10:36 pm
 **/

public class ProducerCallbackDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallbackDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:19092,192.168.1.101:29092,192.168.1.101:39092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record (create message)
        ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "Hello this is a test");

        // send data (async)
        for (int i = 0; i < 20; i++) {
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("receive metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n");
                } else {
                    log.error("Record send failed", exception);
                }
            });

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // flush and close producer (sync)
        producer.flush();
        producer.close();
    }
}
