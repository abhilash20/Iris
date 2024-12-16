package com.data.kafkadataprocessing;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface MongoRep extends MongoRepository<MongoEntity, String> {
}
