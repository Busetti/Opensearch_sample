package demo.search;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

@Configuration
class MongoSetup {
  @Bean ApplicationRunner initializeCollections(MongoTemplate mongo) {
    return args -> {
      if (!mongo.collectionExists(Employee.class)) mongo.createCollection(Employee.class);
      if (!mongo.collectionExists(OutboxEvent.class)) mongo.createCollection(OutboxEvent.class);
      if (!mongo.collectionExists(OutboxPublisher.CHECKPOINT_COLLECTION)) mongo.createCollection(OutboxPublisher.CHECKPOINT_COLLECTION);
      if (!mongo.collectionExists("jobs")) mongo.createCollection("jobs");
      mongo.indexOps(OutboxEvent.class).createIndex(new Index().on("published", Sort.Direction.ASC).on("createdAt", Sort.Direction.ASC));
    };
  }
}
