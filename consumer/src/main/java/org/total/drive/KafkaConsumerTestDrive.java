package org.total.drive;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;
import org.total.constants.Utils;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Pavlo.Fandych
 */
public final class KafkaConsumerTestDrive {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerTestDrive.class);

    private static final Consumer<String, String> CONSUMER = createConsumer();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Shutdown(Thread.currentThread())));

        LOGGER.info("Start!");

        try {
            while (true) {
                final ConsumerRecords<String, String> records = CONSUMER.poll(Duration.ofSeconds(2L));

                records.forEach(record -> LOGGER
                        .info("Value: " + record.value() + " topic: " + record.topic() + " partition: " + record.partition()
                                + " offset: " + record.offset()));
                CONSUMER.commitSync();
            }
        } catch (WakeupException e) {
            /*NOP*/
        } finally {
            CONSUMER.close();
        }
    }

    private static Consumer<String, String> createConsumer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "test_consumer_group");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final Consumer<String, String> result = new KafkaConsumer<>(kafkaProps);
        result.subscribe(Collections.singletonList(Utils.TOPIC_NAME));

        return result;
    }

    private static class Shutdown implements Runnable {

        private Thread main;

        public Shutdown(Thread main) {
            this.main = main;
        }

        @Override
        public void run() {
            LOGGER.info("End!");
            CONSUMER.wakeup();
            try {
                main.join();
            } catch (Exception e) {
                LOGGER.error(e, e);
            }
        }
    }
}
