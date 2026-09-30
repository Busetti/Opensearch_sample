package demo.search;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("outbox")
public record OutboxEvent(@Id String id, String employeeId, String operation,
                          String payload, @Indexed boolean published, Instant createdAt) {}
