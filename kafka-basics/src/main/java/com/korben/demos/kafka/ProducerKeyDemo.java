package com.korben.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

public class ProducerKeyDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerKeyDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:29092,192.168.1.101:39092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send data (async)
        for (int i = 0; i < 10; i++) {

            String topic = "java_demo";
            String value = "Hello World " + i;
            String key = "id_" + i;

            // create producer record (create message)
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("receive metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n");
                } else {
                    log.error("Record send failed", exception);
                }
            });
        }

        // flush and close producer (sync)
        producer.flush();
        producer.close();
    }
}
