package com.data.kafkadataprocessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.springframework.stereotype.Service;


@Service
public class ChangeStreamService {

    private final MongoClient mongoClient;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public ChangeStreamService(MongoClient mongoClient, KafkaProducerService kafkaProducerService, ObjectMapper objectMapper) {
        this.mongoClient = mongoClient;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void watchChanges() {
        new Thread(() -> {
            MongoDatabase database = mongoClient.getDatabase("tms");
            MongoCollection<Document> collection = database.getCollection("users");

            // Start listening to the change stream
            collection.watch().forEach((ChangeStreamDocument<Document> change) -> {
                if ("insert".equals(change.getOperationType().getValue()) || "update".equals(change.getOperationType().getValue())) {
                    Document newDocument = change.getFullDocument();
                    try {
//                        String message = objectMapper.writeValueAsString(newDocument);
                        kafkaProducerService.sendMessage("mongo-to-mysql", newDocument);
                    } catch (Exception e) {
                        e.printStackTrace(); // Add proper error handling here
                    }
                }
            });
        }).start();
    }
}

