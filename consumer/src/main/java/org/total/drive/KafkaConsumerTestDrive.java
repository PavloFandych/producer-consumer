package org.total.drive;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    public static void main(String[] args) {
        final Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(Utils.TOPIC_NAME));

        try {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2L));

            records.forEach(record -> LOGGER
                    .info("Value: " + record.value() + " topic: " + record.topic() + " partition: " + record.partition()
                            + " offset: " + record.offset()));
            consumer.commitSync();
        } finally {
            consumer.close();
        }
    }

    private static Consumer<String, String> createConsumer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "test_consumer_group");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(kafkaProps);
    }
}
