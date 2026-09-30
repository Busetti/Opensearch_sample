package demo.search;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@EnabledIfEnvironmentVariable(named = "RUN_MONGO_IT", matches = "true")
class ChangeStreamRelayTest {
  @Test void backfillsAndResumesEventsCommittedWhileStopped() throws Exception {
    try (var fixture = new Fixture()) {
      var keys = new CopyOnWriteArrayList<String>();
      when(fixture.kafka.send(anyString(), anyString(), anyString())).thenAnswer(call -> {
        keys.add(call.getArgument(1)); return CompletableFuture.completedFuture(null);
      });
      fixture.insert("before-start");
      var relay = fixture.relay();
      relay.start();
      try {
        await(() -> keys.contains("before-start") && relay.status().state().equals("STREAMING"));
        fixture.insert("live-event");
        await(() -> fixture.checkpoint() != null && keys.contains("live-event"));
      } finally { relay.stop(); }
      fixture.insert("during-downtime");
      var restarted = fixture.relay();
      restarted.start();
      try {
        await(() -> keys.contains("during-downtime") && fixture.pending() == 0);
        assertEquals("STREAMING", restarted.status().state());
      } finally { restarted.stop(); }
    }
  }

  @Test void failureDoesNotAdvanceAndCheckpointFailureRollsBackPublishedMarkers() throws Exception {
    try (var fixture = new Fixture()) {
      var event = fixture.insert("ack-test");
      var token = BsonDocument.parse("{\"_data\":\"test-token\"}");
      when(fixture.kafka.send(anyString(), anyString(), anyString()))
          .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));
      var relay = fixture.relay();
      try {
        assertThrows(Exception.class, () -> relay.publishAndCheckpoint(List.of(event), token));
        assertEquals(1, fixture.pending());
        assertNull(fixture.checkpoint());
        when(fixture.kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        fixture.mongo.executeCommand(new Document("collMod", OutboxPublisher.CHECKPOINT_COLLECTION)
            .append("validator", new Document("resumeToken", new Document("$exists", false))));
        assertThrows(Exception.class, () -> relay.publishAndCheckpoint(List.of(event), token));
        assertEquals(1, fixture.pending(), "published marker must roll back if checkpoint cannot commit");
        fixture.mongo.executeCommand(new Document("collMod", OutboxPublisher.CHECKPOINT_COLLECTION).append("validator", new Document()));
        relay.publishAndCheckpoint(List.of(event), token);
        assertEquals(0, fixture.pending());
        assertEquals(token.toJson(), fixture.checkpoint().getString("resumeToken"));
      } finally { relay.stop(); }
    }
  }

  @Test void onlyHistoryLossAllowsAutomaticCheckpointReset() {
    assertTrue(OutboxPublisher.historyLost(new com.mongodb.MongoException(286, "ChangeStreamHistoryLost")));
    assertFalse(OutboxPublisher.historyLost(new com.mongodb.MongoException(260, "Invalid resume token")));
    assertFalse(OutboxPublisher.historyLost(new com.mongodb.MongoException(13, "Unauthorized")));
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail("Timed out waiting for relay");
      Thread.sleep(50);
    }
  }

  private static class Fixture implements AutoCloseable {
    final SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(
        "mongodb://localhost:27017/employee_relay_test?replicaSet=rs0&directConnection=true&w=majority");
    final MongoTemplate mongo = new MongoTemplate(factory);
    @SuppressWarnings("unchecked") final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    Fixture() {
      mongo.getDb().drop(); mongo.createCollection(OutboxEvent.class);
      mongo.createCollection(OutboxPublisher.CHECKPOINT_COLLECTION);
    }
    OutboxPublisher relay() { return new OutboxPublisher(mongo, kafka,
        new TransactionTemplate(new MongoTransactionManager(factory)), "test-topic"); }
    OutboxEvent insert(String key) {
      var event = new OutboxEvent(UUID.randomUUID().toString(), key, "UPSERT", "{\"id\":\""+key+"\"}", false, Instant.now());
      return mongo.insert(event);
    }
    long pending() { return mongo.getCollection("outbox").countDocuments(new Document("published", false)); }
    Document checkpoint() { return mongo.findById(OutboxPublisher.CHECKPOINT_ID, Document.class, OutboxPublisher.CHECKPOINT_COLLECTION); }
    public void close() throws Exception { mongo.getDb().drop(); factory.destroy(); }
  }
}
