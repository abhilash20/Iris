package com.data.kafkadataprocessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataMigrationService {

    private final MongoRep mongoRepository;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    public DataMigrationService(MongoRep mongoRepository, KafkaProducerService kafkaProducerService, ObjectMapper objectMapper) {
        this.mongoRepository = mongoRepository;
        this.kafkaProducerService = kafkaProducerService;
        this.objectMapper = objectMapper;
    }

//    @Scheduled(fixedRate = 5000)
//    @Async
    public void migrateData() throws Exception {
        List<MongoEntity> entities = mongoRepository.findAll();
        for (MongoEntity entity : entities) {
//            String message = objectMapper.writeValueAsString(entity.toString());
            kafkaProducerService.sendMessage("mongo-to-mysql", entity);
        }
    }
}

