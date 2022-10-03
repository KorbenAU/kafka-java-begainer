package com.korben.demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @ClassName ProducerDemo
 * @Description TODO
 * @Author Korben Gao
 * @Date 29/9/2022 10:36 pm
 **/

public class ConsumerDemoCooperativeRebalance {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperativeRebalance.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting consumer");

        String topic = "java_demo";

        // create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:29092,192.168.1.101:39092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none | earliest | latest
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // create consuer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // ger reference to the current thread
        final Thread thread = Thread.currentThread();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to the topics
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                log.info("Polling message");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("[key: " + record.key() + "] [offset: " + record.offset() + "] [partition: " + record.partition() + "] " + record.value());
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            log.info("consumer is closed");
        }
    }
}
