package com.data.kafkadataprocessing;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
public class KafkaConsumer {

    @KafkaListener(topics = "mongo-to-mysql", groupId = "test-group")
    public void listen(Object message) {
        System.out.println("Received from Kafka: " + message.toString());
    }
}