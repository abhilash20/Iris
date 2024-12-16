package com.data.kafkadataprocessing;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class KafkaProducerService {

    Logger logger = Logger.getLogger(KafkaProducerService.class.getName());
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, Object message) {

        System.out.println("Message sent to topic " + topic + " : " + message);
        kafkaTemplate.send(topic, message);

    }

    public Logger getLogger() {
        return logger;
    }
}

