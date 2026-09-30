package demo.search;

import com.mongodb.MongoException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Single-owner relay: committed outbox inserts -> Kafka -> durable MongoDB checkpoint. */
@Component
public class OutboxPublisher {
  static final String CHECKPOINT_COLLECTION = "relay_checkpoints";
  static final String CHECKPOINT_ID = "outbox-to-kafka-v1";
  private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
  private final MongoTemplate mongo;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionTemplate transactions;
  private final String topic;
  private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> new Thread(r, "outbox-change-stream"));
  private volatile MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
  private volatile boolean running;
  private volatile String state = "STARTING";
  private volatile String lastError;
  private volatile Instant lastCheckpointAt;
  private volatile long delivered;
  private volatile long recoveries;

  public OutboxPublisher(MongoTemplate mongo, KafkaTemplate<String, String> kafka,
      TransactionTemplate transactions, @Value("${app.topic}") String topic) {
    this.mongo = mongo; this.kafka = kafka; this.transactions = transactions; this.topic = topic;
  }

  @EventListener(ApplicationReadyEvent.class)
  public synchronized void start() {
    if (running) return;
    running = true;
    worker.submit(this::run);
  }

  private void run() {
    while (running) {
      try {
        Document saved = mongo.findById(CHECKPOINT_ID, Document.class, CHECKPOINT_COLLECTION);
        BsonTimestamp fence = null;
        BsonDocument token = saved == null ? null : BsonDocument.parse(saved.getString("resumeToken"));
        if (token == null) {
          state = "RECOVERING";
          // Fence BEFORE the snapshot: concurrent inserts are also covered by the stream.
          fence = mongo.getDb().runCommand(new BsonDocument("hello", new BsonInt32(1)), BsonDocument.class)
              .getDocument("$clusterTime").getTimestamp("clusterTime");
          recoverPending();
        }
        stream(token, fence);
      } catch (Exception e) {
        if (!running) break;
        lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
        state = "RETRYING";
        log.warn("Outbox relay will retry without advancing its checkpoint: {}", lastError);
        // A valid but expired token can be recovered using durable unpublished events.
        // Malformed tokens, permissions and collection invalidation are not silently ignored.
        if (historyLost(e)) {
          try {
            mongo.remove(Query.query(Criteria.where("_id").is(CHECKPOINT_ID)), CHECKPOINT_COLLECTION);
            recoveries++;
          } catch (Exception cleanup) { log.warn("Could not reset expired checkpoint", cleanup); }
        }
        try { Thread.sleep(2000); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
      } finally {
        var active = cursor;
        cursor = null;
        if (active != null) { try { active.close(); } catch (Exception ignored) { } }
      }
    }
    state = "STOPPED";
  }

  private void recoverPending() throws Exception {
    while (running) {
      List<OutboxEvent> batch = mongo.find(Query.query(Criteria.where("published").is(false))
          .with(Sort.by("createdAt")).limit(500), OutboxEvent.class);
      if (batch.isEmpty()) return;
      publishAndCheckpoint(batch, null);
    }
  }

  private void stream(BsonDocument token, BsonTimestamp fence) throws Exception {
    var watch = mongo.getCollection("outbox")
        .watch(List.of(Aggregates.match(Filters.eq("operationType", "insert"))))
        .batchSize(500).maxAwaitTime(200, TimeUnit.MILLISECONDS);
    if (token != null) watch = watch.resumeAfter(token);
    else watch = watch.startAtOperationTime(fence);
    try (var opened = watch.cursor()) {
      cursor = opened;
      state = "STREAMING";
      lastError = null;
      long lastIdleCheckpoint = System.nanoTime();
      while (running) {
        List<OutboxEvent> batch = new ArrayList<>(500);
        while (running && batch.size() < 500) {
          var change = opened.tryNext(); // MongoDB awaits changes; no recurring collection query.
          if (change == null) break;
          Document document = change.getFullDocument();
          if (document == null) throw new IllegalStateException("Outbox insert has no full document");
          batch.add(mongo.getConverter().read(OutboxEvent.class, document));
        }
        BsonDocument nextToken = opened.getResumeToken();
        if (!batch.isEmpty()) {
          publishAndCheckpoint(batch, nextToken);
          lastIdleCheckpoint = System.nanoTime();
        } else if (running && nextToken != null && System.nanoTime() - lastIdleCheckpoint > TimeUnit.SECONDS.toNanos(10)) {
          // Idle high-water marks keep the token fresh as unrelated oplog entries advance.
          publishAndCheckpoint(List.of(), nextToken);
          lastIdleCheckpoint = System.nanoTime();
        }
      }
    }
  }

  void publishAndCheckpoint(List<OutboxEvent> events, BsonDocument token) throws Exception {
    CompletableFuture<?>[] sends = events.stream()
        .map(e -> kafka.send(topic, e.employeeId(), e.payload())).toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(sends).get(60, TimeUnit.SECONDS);
    // Never acknowledge an event or advance the token before ALL Kafka sends succeed.
    transactions.executeWithoutResult(status -> {
      if (!events.isEmpty()) mongo.updateMulti(
          Query.query(Criteria.where("_id").in(events.stream().map(OutboxEvent::id).toList())),
          Update.update("published", true), OutboxEvent.class);
      if (token != null) mongo.save(new Document("_id", CHECKPOINT_ID)
          .append("resumeToken", token.toJson()).append("updatedAt", java.util.Date.from(Instant.now())),
          CHECKPOINT_COLLECTION);
    });
    delivered += events.size();
    if (token != null) lastCheckpointAt = Instant.now();
  }

  static boolean historyLost(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof MongoException mongoError && mongoError.getCode() == 286) return true;
    }
    return false;
  }

  public Status status() { return new Status(state, delivered, recoveries, lastCheckpointAt, lastError); }
  public record Status(String state, long deliveredSinceStart, long historyRecoveries,
                       Instant lastCheckpointAt, String lastError) { }

  @PreDestroy
  public synchronized void stop() {
    running = false;
    worker.shutdownNow();
    // Cursor is closed by its owner to avoid concurrent use of the MongoDB cursor.
    try { worker.awaitTermination(5, TimeUnit.SECONDS); }
    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
  }
}
